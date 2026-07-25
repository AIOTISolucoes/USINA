/* ============================================================================
   EMISSOR DO PROTOCOLO V2 DO GATEWAY (Modbus/MQTT V16 - WAGO CC100)

   COPIA FIEL do bloco "EMISSOR DO PROTOCOLO V2" do controle_gateway_v16.html
   que o Igor enviou em 25/07/2026. A documentacao do gateway (cap. 7) manda
   EXPRESSAMENTE reusar estas funcoes em vez de reimplementar: o gateway
   confere CRC32 e SHA-256 sobre os bytes exatos dos puts, entao qualquer
   diferenca de serializacao (ordem de chave, espaco, formato de numero)
   faz a transacao inteira ser rejeitada no validate.

   NAO EDITAR a logica daqui. Se o Igor mandar uma versao nova do
   controle_gateway_v16.html, re-extraia este bloco inteiro.

   O que ficou de fora de proposito: a UI e o cliente MQTT do arquivo dele.
   Na nossa plataforma quem publica no IoT Core e o BACKEND (Lambda), nao o
   navegador - assim nao existe credencial de broker no browser.

   Uso:
     const txn = buildTransaction(config, "cfg-" + Date.now(), {usina: "Bancada"});
     // txn.publications -> [{topic, payload}, ...] ja na ordem certa
   ============================================================================ */

/* ============================================================================
   EMISSOR DO PROTOCOLO V2 (porte fiel de gateway_configurator/mqtt_entities.py)
   ==========================================================================*/
const SCHEMA_VERSION="1.0.0", PROTOCOL_VERSION=2, MAX_TOKENS=96, MAX_VALUES=48, MAX_PAYLOAD_BYTES=511;
const SAFE_ID=/^[A-Za-z0-9._-]{1,63}$/;
const TRANSPORT={rtu:0,serial:0,tcp:1,ethernet:1};
const PARITY={none:0,odd:1,even:2};
const COMPARE={"==":0,"=":0,"!=":1,"<>":1,">":2,">=":3,"<":4,"<=":5,bit_set:6,bit_clear:7,between:8,outside:9};
const SEVERITY={info:0,warning:1,alarm:2,error:2,critical:3};
const SEQOP={write:1,double_bit:1,delay:2,pulse:3,verify:4,condition:5};

function isNum(v){return typeof v==="number"&&isFinite(v);}
function _number(v,d){ if(v===null||v===undefined||v==="")return d; return isNum(v)?v:d; }
function intn(v,d){ return Math.trunc(_number(v,d)); }
function _word(v){ if(typeof v==="boolean")return v?1:0; if(isNum(v))return Math.trunc(v)&0xFFFF; return 0; }
function _meta(r,k,d){ const m=(r&&typeof r.metadata==="object"&&r.metadata)?r.metadata:{}; return (k in m)?m[k]:d; }
function _metadata(r){ return (r&&typeof r.metadata==="object"&&r.metadata)?r.metadata:{}; }
function _records(cfg,s){ const v=cfg[s]; return Array.isArray(v)?v.filter(x=>x&&typeof x==="object"):[]; }
function _fixedText(e){ const t=String(e||""); if(t.length>=2&&t[0]===t[t.length-1]&&(t[0]==="'"||t[0]==='"'))return t.slice(1,-1); return t; }
function _valueType(f){
  const dt=String(f.data_type||"uint16").toLowerCase();
  const sw=["swapped","cdab","dcba","little_endian"].includes(String(f.word_order||"normal").toLowerCase());
  if(dt==="bool")return 5; if(dt==="int16")return 1;
  if(dt==="uint32")return sw?6:2; if(dt==="int32")return sw?7:3; if(dt==="float32")return sw?8:4;
  return 0;
}
function _fieldSource(f){
  const s=String(f.source_type||"modbus").toLowerCase();
  if(s==="modbus")return[0,0]; if(s==="static")return[2,0]; if(s==="derived")return[3,0];
  if(s==="timestamp")return[4,2]; if(s==="quality")return[4,3]; return[2,0];
}
function _slotMap(records,section,max){
  if(records.length>max)throw new Error(`${section} tem ${records.length} registros; staging suporta ${max}`);
  const r={}; records.forEach((rec,i)=>{ const id=String(rec.id||""); if(!id)throw new Error(`${section} linha ${i+1} sem id`); if(id in r)throw new Error(`${section} id duplicado: ${id}`); r[id]=i+1; }); return r;
}
function _topics(cfg,usina){
  const g=cfg.general||{},p=cfg.plant||{};
  const seg=String(usina||g.plant_id||p.id||"Naturagua");
  if(!seg||/[\/#+ ]/.test(seg))throw new Error("segmento de usina inválido: "+seg);
  const base=`dev/write/UFV/${seg}/gateway/configuration`;
  return {request:`${base}/request`,chunk:`${base}/chunk`,feedback:`${base}/feedback`};
}
function _topicByDevice(cfg){ const r={}; for(const t of _records(cfg,"topics")){ const d=String(t.device_id||""); if(d){ (r[d]=r[d]||{})[String(t.purpose||"").toLowerCase()]=t; } } return r; }

function jsonTokens(v){
  if(v&&typeof v==="object"&&!Array.isArray(v))return 1+Object.values(v).reduce((a,x)=>a+1+jsonTokens(x),0);
  if(Array.isArray(v))return 1+v.reduce((a,x)=>a+jsonTokens(x),0);
  return 1;
}
// JSON canônico ASCII: chaves ordenadas, sem espaços — como json.dumps(sort_keys,separators,ensure_ascii)
function canon(v){
  if(v===null)return "null";
  if(typeof v==="boolean")return v?"true":"false";
  if(typeof v==="number"){ if(!isFinite(v))throw new Error("número inválido"); return numStr(v); }
  if(typeof v==="string")return qStr(v);
  if(Array.isArray(v))return "["+v.map(canon).join(",")+"]";
  const ks=Object.keys(v).sort();
  return "{"+ks.map(k=>qStr(k)+":"+canon(v[k])).join(",")+"}";
}
function numStr(n){ if(Number.isInteger(n))return String(n); let s=String(n); return s; }
function qStr(s){
  let o='"';
  for(const ch of String(s)){
    const c=ch.codePointAt(0);
    if(ch==='"')o+='\\"'; else if(ch==='\\')o+='\\\\';
    else if(c===8)o+='\\b'; else if(c===9)o+='\\t'; else if(c===10)o+='\\n'; else if(c===12)o+='\\f'; else if(c===13)o+='\\r';
    else if(c<0x20||c>0x7e)o+='\\u'+c.toString(16).padStart(4,'0');
    else o+=ch;
  }
  return o+'"';
}
function makeMessage(rid,seq,command,{entity="",slot=0,sub=0,values=[]}={}){
  if(!SAFE_ID.test(rid))throw new Error("request_id inválido (1..63 de A-Z a-z 0-9 . _ -)");
  if(values.length>MAX_VALUES)throw new Error(`values tem ${values.length} itens; limite ${MAX_VALUES}`);
  return {command,count:values.length,entity,request_id:rid,schema_version:SCHEMA_VERSION,seq,slot,sub,v:PROTOCOL_VERSION,values};
}

/* ---- gerador de entidades: espelha configuration_entity_specs() ---- */
function configurationEntitySpecs(cfg){
  const channels=_records(cfg,"channels"),devices=_records(cfg,"devices"),templates=_records(cfg,"templates"),
        requests=_records(cfg,"requests"),fields=_records(cfg,"fields"),commands=_records(cfg,"commands"),
        alarms=_records(cfg,"alarms"),events=_records(cfg,"events"),sequences=_records(cfg,"sequences"),pids=_records(cfg,"pid");
  const channelSlots=_slotMap(channels,"channels",32),deviceSlots=_slotMap(devices,"devices",255),
        templateSlots=_slotMap(templates,"templates",100),sequenceSlots=_slotMap(sequences,"sequences",128),
        pidSlots=_slotMap(pids,"pid",16);
  const fieldById={}; fields.forEach(f=>fieldById[String(f.id||"")]=f);
  const deviceTopics=_topicByDevice(cfg);
  const reqByTpl={},fldByTpl={},requestSub={},fieldSub={};
  requests.forEach(r=>{ (reqByTpl[String(r.template_id||"")]=reqByTpl[String(r.template_id||"")]||[]).push(r); });
  fields.forEach(f=>{ (fldByTpl[String(f.template_id||"")]=fldByTpl[String(f.template_id||"")]||[]).push(f); });
  for(const t in reqByTpl){ const rows=reqByTpl[t]; if(rows.length>32)throw new Error(`templates[${t}] tem ${rows.length} requests; limite 32`); rows.forEach((r,i)=>requestSub[String(r.id||"")]=i+1); }
  for(const t in fldByTpl){ const rows=fldByTpl[t]; if(rows.length>192)throw new Error(`templates[${t}] tem ${rows.length} fields; limite 192`); rows.forEach((r,i)=>fieldSub[String(r.id||"")]=i+1); }

  const specs=[]; const add=(section,id,entity,slot,sub,values)=>specs.push({section,id,entity,slot,sub,values});
  const plant=(cfg.plant&&typeof cfg.plant==="object")?cfg.plant:{}; const pm=_metadata(plant); const plantId=String(plant.id||"Naturagua");
  add("plant",plantId,"plant",0,0,[true,plantId,String(plant.name||""),String(plant.company||""),String(plant.client||""),
    String(pm.complex||"UFV"),String(pm.area||""),String(pm.country||"BR"),String(pm.state||""),String(pm.city||""),
    String(plant.timezone||"America/Fortaleza"),String(pm.topic_slug||plantId),_number(plant.latitude,0.0),_number(plant.longitude,0.0),_number(pm.nominal_power_kw,0.0)]);

  channels.forEach((c,i)=>{
    const slot=i+1,id=String(c.id||slot),tr=String(c.transport||"rtu").toLowerCase();
    if(!(tr in TRANSPORT))throw new Error(`channels[${id}] transport não representável: ${tr}`);
    let sp=c.serial_port||0; if(typeof sp==="string"){ const m=sp.match(/(\d+)$/); sp=m?parseInt(m[1]):0; }
    add("channels",id,"channel",slot,0,[!!(c.enabled??true),String(c.name||""),TRANSPORT[tr],intn(sp,0),intn(c.baudrate,0),
      intn(c.data_bits,8),PARITY[String(c.parity||"none").toLowerCase()]??0,intn(c.stop_bits,1),String(c.ip||""),intn(c.port,502),
      intn(c.timeout_ms,1000),intn(c.retries,1),intn(c.poll_interval_ms,1000),intn(_meta(c,"inter_device_ms",0),0),!!_meta(c,"fast_skip_offline",false)]);
  });

  templates.forEach((t,i)=>{
    const slot=i+1,id=String(t.id||slot),treq=reqByTpl[id]||[],tfld=fldByTpl[id]||[];
    const statusFields=tfld.filter(f=>f.status_mapping&&typeof f.status_mapping==="object"&&Object.keys(f.status_mapping).length);
    const mapVerified=treq.concat(tfld).every(r=>String(r.validation_status||"verified")==="verified");
    const totalWords=treq.reduce((mx,r)=>Math.max(mx,intn(r.buffer_offset,0)+intn(r.quantity,0)),0);
    add("templates",id,"template",slot,0,[!!(t.enabled??true),mapVerified,String(t.name||""),String(t.version||1),String(t.device_type||""),
      treq.length,tfld.length,totalWords, statusFields.length?(fieldSub[String(statusFields[0].id)]||0):0,
      statusFields.reduce((a,f)=>a+Object.keys(f.status_mapping||{}).length,0)]);
    treq.forEach((r,j)=>add("requests",String(r.id||j+1),"request",slot,j+1,[!!(r.enabled??true),intn(r.function_code,0),intn(r.address,0),intn(r.quantity,0),intn(r.buffer_offset,0)]));
    let statusIndex=0;
    tfld.forEach((f,j)=>{
      const fid=String(f.id||j+1),reqId=String(f.request_id||"");
      const req=treq.find(x=>String(x.id)===reqId)||{};
      const [source,systemValue]=_fieldSource(f); const dv=(f.default_value!==undefined)?f.default_value:0;
      const fixedText=source===2?_fixedText(f.source_expression||""):""; const fixedValue=(source===2&&!fixedText)?_number(dv,0.0):0.0;
      const quote=String(f.data_type||"").toLowerCase()==="string"||!!fixedText;
      add("fields",fid,"field",slot,j+1,[!!(f.enabled??true),!!_meta(f,"publish",true),String(f.json_key||""),String(f.unit||""),quote,
        intn(_meta(f,"decimals",3),3),!!_meta(f,"address_based",false),requestSub[reqId]||0,intn(req.address,0)+intn(f.register_offset,0),
        intn(f.register_offset,0),_valueType(f),_number(f.gain,1.0),_number(f.offset,0.0),intn(f.bit_offset,0),source,String(f.source_expression||""),
        fixedValue,fixedText,_number(dv,0.0),(typeof dv==="string")?dv:"",systemValue,intn(_meta(f,"related_role",0),0),
        intn(_meta(f,"max_age_seconds",0),0),intn(_meta(f,"on_stale",0),0),intn(_meta(f,"calc_operation",0),0),
        fieldSub[String(_meta(f,"calc_field_a",""))]||0,fieldSub[String(_meta(f,"calc_field_b",""))]||0]);
      const mp=f.status_mapping; if(mp&&typeof mp==="object"){ for(const raw in mp){ statusIndex++; if(statusIndex>16)throw new Error(`templates[${id}] tem mais de 16 correlações de status`); add("fields",`${fid}.status.${raw}`,"status_map",slot,statusIndex,[true,parseInt(raw),parseInt(mp[raw])]); } }
    });
  });

  devices.forEach((d,i)=>{
    const slot=i+1,id=String(d.id||slot),tp=deviceTopics[id]||{};
    const tel=tp.telemetry||{};
    add("devices",id,"device",slot,0,[!!(d.enabled??true),id,String(d.description||d.name||""),String(_meta(d,"domain","")),String(_meta(d,"complex","")),
      String(_meta(d,"power_plant",plantId)),String(d.manufacturer||""),String(d.model||""),String(d.device_type||""),String(_meta(d,"energy_source","")),
      channelSlots[String(d.channel_id||"")]||0,intn(d.unit_id,0),templateSlots[String(d.template_id||"")]||0,intn(d.poll_interval_ms,1000),
      intn(d.publish_interval_ms,10000),intn(d.stale_timeout_ms,30000),"","","",!!(d.commands_enabled??false),
      intn(tel.qos,1),!!(tel.retain??false),!!_meta(d,"allow_unverified_map",false),
      deviceSlots[String(_meta(d,"linked_string_box",""))]||0,deviceSlots[String(_meta(d,"related_meter",""))]||0,
      deviceSlots[String(_meta(d,"related_weather",""))]||0,deviceSlots[String(_meta(d,"related_relay",""))]||0,intn(_meta(d,"max_sync_skew_ms",0),0)]);
  });

  const g=(cfg.general&&typeof cfg.general==="object")?cfg.general:{};
  const globalTopics=[String(g.command_feedback_topic||""),String(g.config_feedback_topic||""),String(g.config_command_topic||"")];
  for(const t of _records(cfg,"topics")){
    const tid=String(t.id||""),did=String(t.device_id||""),purpose=String(t.purpose||"").toLowerCase();
    if(!did){ if(globalTopics.includes(String(t.topic||"")))continue; throw new Error(`topics[${tid}] global não representável no staging: ${purpose}`); }
    if(!["telemetry","command","feedback","ack"].includes(purpose))throw new Error(`topics[${tid}] purpose não suportado: ${purpose}`);
    add("topics",tid,"topic",deviceSlots[did]||0,0,[purpose,String(t.topic||"")]);
  }

  const cmdByDev={}; commands.forEach(c=>{ (cmdByDev[String(c.device_id||"")]=cmdByDev[String(c.device_id||"")]||[]).push(c); });
  for(const did in cmdByDev){ const rows=cmdByDev[did];
    if(!did||!(did in deviceSlots))throw new Error(`commands ligados a device inexistente: ${did}`);
    if(rows.length>6)throw new Error(`devices[${did}] tem ${rows.length} commands; limite 6`);
    rows.forEach((c,j)=>{ let fixed=(c.fixed_value!==undefined)?c.fixed_value:0; const raw=Array.isArray(fixed)?fixed:[fixed];
      const words=raw.slice(0,16).map(_word); while(words.length<16)words.push(0);
      const qty=[15,16].includes(intn(c.function_code,0))?raw.length:1;
      add("commands",String(c.id||j+1),"command",deviceSlots[did],j+1,[!!(c.enabled??false),String(c.name||""),intn(c.function_code,0),
        intn(c.address,0),qty,words[0],!!(c.use_payload??false),intn(_meta(c,"min",0),0),intn(_meta(c,"max",65535),65535),...words]);
    });
  }

  alarms.forEach((a,i)=>{ const slot=i+1,th=(a.threshold!==undefined)?a.threshold:0;
    add("alarms",String(a.id||slot),"alarm",slot,0,[!!(a.enabled??true),slot,String(a.name||""),deviceSlots[String(a.device_id||"")]||0,String(a.field_id||""),
      COMPARE[String(a.condition||"==").toLowerCase()]??0,_number(th,0.0),_number(_meta(a,"threshold_b",th),0.0),_number(_meta(a,"hysteresis",0),0.0),
      intn(a.delay_ms,0),intn(_meta(a,"delay_off_ms",0),0),SEVERITY[String(a.severity||"warning").toLowerCase()]??1,!!_meta(a,"latch",false),String(a.message||""),String(_meta(a,"topic",""))]);
  });
  events.forEach((e,i)=>{ const slot=i+1;
    add("events",String(e.id||slot),"event",slot,0,[!!(e.enabled??true),slot,String(e.name||""),deviceSlots[String(e.device_id||"")]||0,String(e.field_id||""),
      COMPARE[String(e.condition||"==").toLowerCase()]??0,_number(_meta(e,"expected_a",0),0.0),_number(_meta(e,"expected_b",0),0.0),intn(_meta(e,"event_code",slot),slot),
      String(e.message||""),String(_meta(e,"topic","")),intn(_meta(e,"qos",1),1),!!_meta(e,"retain",false)]);
  });

  sequences.forEach((s,i)=>{ const slot=i+1,id=String(s.id||slot),steps=Array.isArray(s.steps)?s.steps:[];
    if(steps.length>16)throw new Error(`sequences[${id}] tem ${steps.length} passos; limite 16`);
    add("sequences",id,"sequence",slot,0,[!!(s.enabled??false),slot,String(s.name||""),String(s.command_name||""),deviceSlots[String(s.device_id||"")]||0,
      steps.length,intn(s.max_retries,0),intn(s.timeout_ms,30000),!!_meta(s,"require_online",true),String(_meta(s,"feedback_topic",g.command_feedback_topic||""))]);
    steps.forEach((st,j)=>{ const type=String(st.type||"").toLowerCase(); let raw=(st.values!==undefined)?st.values:[(st.value!==undefined)?st.value:0]; raw=Array.isArray(raw)?raw:[raw];
      const words=raw.slice(0,16).map(_word); while(words.length<16)words.push(0);
      const f=fieldById[String(st.field_id||"")]||{}; const fr=requests.find(r=>String(r.id)===String(f.request_id||""))||{};
      const confAddr=intn(st.confirm_address,intn(fr.address,0)+intn(f.register_offset,0));
      add("sequences",`${id}.step.${j+1}`,"sequence_step",slot,j+1,[true,j+1,String(st.description||""),SEQOP[type]??0,intn(st.function_code,0),intn(st.address,0),
        (["delay","verify","condition"].includes(type)?0:raw.length),...words,intn(st.delay_ms,0),intn(st.pulse_ms,0),intn(st.retries,0),intn(st.confirm_function_code,3),
        confAddr,intn(st.confirm_quantity,1),COMPARE[String(st.operator||"==").toLowerCase()]??0,_word(st.value),intn(st.mask,0),intn(st.expected_high,0),intn(st.next_success,0),intn(st.next_failure,0)]);
    });
  });

  pids.forEach((p,i)=>{ const slot=i+1,id=String(p.id||slot); const pvf=fieldById[String(p.process_field_id||"")]||{};
    const pvTpl=String(pvf.template_id||""); const infDev=devices.find(d=>String(d.template_id)===pvTpl)||{};
    const pvDev=String(_meta(p,"pv_device_id",infDev.id||"")); let targets=_meta(p,"targets",[]); targets=Array.isArray(targets)?targets:[];
    add("pid",id,"pid",slot,0,[!!(p.enabled??false),slot,String(p.name||""),deviceSlots[pvDev]||0,String(pvf.json_key||p.process_field_id||""),
      _number(p.setpoint,0.0),_number(p.kp,0.0),_number(p.ki,0.0),_number(p.kd,0.0),intn(p.sample_ms,1000),_number(p.output_min,0.0),_number(p.output_max,100.0),
      _number(p.ramp_up,0.0),_number(p.ramp_down,0.0),_number(p.deadband,0.0),String(p.mode||"manual").toLowerCase()==="manual",_number(_meta(p,"manual_output",0),0.0),
      _number(p.safe_output,0.0),intn(p.stale_timeout_ms,30000),targets.length]);
    targets.forEach((tg,j)=>{ if(!tg||typeof tg!=="object")throw new Error(`pid[${id}] target ${j+1} não é objeto`);
      add("pid",`${id}.target.${j+1}`,"pid_target",slot,j+1,[!!(tg.enabled??true),deviceSlots[String(tg.device_id||"")]||0,_number(tg.nominal_power_kw,0.0),
        _number(tg.minimum_percent,0.0),_number(tg.maximum_percent,100.0),sequenceSlots[String(tg.sequence_id||"")]||0,intn(tg.command_index,0)]);
    });
  });

  const rc=(cfg.auto_reclosing&&typeof cfg.auto_reclosing==="object")?cfg.auto_reclosing:{};
  const sched=(rc.allowed_schedule&&typeof rc.allowed_schedule==="object")?rc.allowed_schedule:{}; const relayId=String(rc.device_id||"");
  add("auto_reclosing",relayId||"auto_reclosing","auto_reclose",0,0,[!!(rc.enabled??false),deviceSlots[relayId]||0,sequenceSlots[String(_meta(rc,"close_sequence_id",""))]||0,
    deviceSlots[relayId]||0,String(rc.breaker_state_field_id||""),_number(_meta(rc,"breaker_open_value",0),0.0),_number(_meta(rc,"breaker_closed_value",1),1.0),
    deviceSlots[String(_meta(rc,"voltage_device_id",""))]||0,String(_meta(rc,"voltage_field_id","")),_number(_meta(rc,"voltage_min",0),0.0),_number(_meta(rc,"voltage_max",0),0.0),
    intn(rc.delay_ms,30000),intn(rc.max_attempts,0),intn(rc.retry_interval_ms,60000),intn(rc.stability_ms,60000),intn(sched.start_hour,0),intn(sched.end_hour,23),
    !!(rc.require_voltage_normal??true),!!(rc.require_communication??true),!!_meta(rc,"require_breaker_open",true),!!_meta(rc,"manual_block",false),0]);
  const rules=[]; const allowed=Array.isArray(rc.allowed_protections)?rc.allowed_protections:[]; const blocked=Array.isArray(rc.blocking_conditions)?rc.blocking_conditions:[];
  allowed.forEach(x=>rules.push([x,true])); blocked.forEach(x=>rules.push([x,false]));
  if(rules.length>32)throw new Error(`auto_reclosing tem ${rules.length} regras; limite 32`);
  rules.forEach((pair,i)=>{ const [rule,allow]=pair; const data=(rule&&typeof rule==="object")?rule:{name:String(rule)}; const rid2=String(data.id||`rule-${i+1}`);
    add("auto_reclosing",rid2,"reclose_rule",i+1,0,[true,i+1,String(data.name||rid2),deviceSlots[String(data.device_id||relayId)]||0,String(data.field_id||""),
      COMPARE[String(data.operator||"==").toLowerCase()]??0,intn(data.value,0),intn(data.mask,0),!!(data.allow??allow),String(data.block_reason||"")]);
  });

  const tp2=_topics(cfg); const gm=_metadata(g);
  add("general",String(g.gateway_id||"gateway"),"general",0,0,[!!gm.auto_topics,String(gm.telemetry_pattern||"dev/read/UFV/{plant}/{type}/{id}"),
    String(gm.command_pattern||"dev/write/UFV/{plant}/{type}/{id}"),String(g.command_feedback_topic||`dev/write/UFV/${plantId}/feedback`),intn(g.default_qos,1),
    !!(g.default_retain??false),!!g.command_subscribe_filter,String(g.command_subscribe_filter||""),intn(g.default_qos,1),true,tp2.request,tp2.chunk,tp2.feedback,
    intn(g.default_qos,1),!!(g.scada_server_enabled??true),intn(g.scada_port,502),String(gm.scada_interface||"")]);

  // metadados
  const mslot=[1];
  const addMeta=(ownerType,ownerSlot,ownerSub,recId,values)=>{ for(const k of Object.keys(values).sort()){ if(mslot[0]>1024)throw new Error("metadata excedeu 1024 entradas"); const val=values[k];
    // str() do Python: bool -> "True"/"False"; dict/list -> JSON canônico; resto -> texto.
    const text=(val&&typeof val==="object")?canon(val):(typeof val==="boolean"?(val?"True":"False"):String(val));
    add("metadata",`${recId}.metadata.${k}`,"metadata",mslot[0],0,[ownerType,ownerSlot,ownerSub,String(k),text]); mslot[0]++; } };
  addMeta("plant",0,0,plantId,pm);
  [["channels",channels,channelSlots],["templates",templates,templateSlots],["devices",devices,deviceSlots],["sequences",sequences,sequenceSlots],["pid",pids,pidSlots]].forEach(([section,rows,slots])=>{
    rows.forEach(r=>{ const rid2=String(r.id||""); addMeta(section.slice(0,-1),slots[rid2]||0,0,rid2,_metadata(r)); }); });
  templates.forEach(t=>{ const tid=String(t.id||""),ts=templateSlots[tid]||0;
    (reqByTpl[tid]||[]).forEach(r=>{ const rid2=String(r.id||""); addMeta("request",ts,requestSub[rid2]||0,rid2,_metadata(r)); });
    (fldByTpl[tid]||[]).forEach(r=>{ const rid2=String(r.id||""); addMeta("field",ts,fieldSub[rid2]||0,rid2,_metadata(r)); }); });
  addMeta("general",0,0,String(g.gateway_id||"gateway"),gm);
  return specs;
}

function buildTransaction(cfg,rid,{usina,intent="apply",timeoutMs=120000,originUser="painel-web"}={}){
  const specs=configurationEntitySpecs(cfg);
  const topics=_topics(cfg,usina);
  const puts=[],raws=[],issues=[];
  specs.forEach((sp,i)=>{ const msg=makeMessage(rid,i+1,"put",{entity:sp.entity,slot:sp.slot,sub:sp.sub,values:sp.values});
    const raw=canon(msg),size=(new TextEncoder().encode(raw)).length,toks=jsonTokens(msg);
    if(size>MAX_PAYLOAD_BYTES||toks>MAX_TOKENS)issues.push(`${sp.section}[${sp.id}] ${sp.entity} slot=${sp.slot} sub=${sp.sub}: ${size}B/${toks}tok`);
    puts.push({spec:sp,msg,raw});
  });
  const body=puts.map(p=>p.raw).join("");
  const crc=crc32(body); const sha=sha256Hex(body).toUpperCase();
  const begin=makeMessage(rid,0,"begin",{values:[intent,specs.length,crc.toString(16).toUpperCase().padStart(8,"0"),sha,timeoutMs,originUser]});
  const validate=makeMessage(rid,specs.length+1,"validate");
  const commit=makeMessage(rid,specs.length+2,"commit");
  return {begin,puts,validate,commit,topics,issues,crc:crc.toString(16).toUpperCase().padStart(8,"0"),specs};
}

/* ---- CRC32 (IEEE, = zlib.crc32) ---- */
const CRC_TAB=(()=>{ const t=new Uint32Array(256); for(let n=0;n<256;n++){ let c=n; for(let k=0;k<8;k++)c=(c&1)?(0xEDB88320^(c>>>1)):(c>>>1); t[n]=c>>>0; } return t; })();
function crc32(str){ const b=new TextEncoder().encode(str); let c=0xFFFFFFFF; for(let i=0;i<b.length;i++)c=CRC_TAB[(c^b[i])&0xFF]^(c>>>8); return (c^0xFFFFFFFF)>>>0; }
/* ---- SHA-256 (síncrono, sem dependência de contexto seguro) ---- */
function sha256Hex(ascii){
  function rotr(n,x){return(x>>>n)|(x<<(32-n));}
  const K=[0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2];
  let H=[0x6a09e667,0xbb67ae85,0x3c6ef372,0xa54ff53a,0x510e527f,0x9b05688c,0x1f83d9ab,0x5be0cd19];
  const bytes=new TextEncoder().encode(ascii); const l=bytes.length;
  const padLen=(56-(l+1)%64+64)%64; const total=l+1+padLen+8; // total sempre múltiplo de 64
  const m=new Uint8Array(total); m.set(bytes); m[l]=0x80; const bl=l*8; const dv=new DataView(m.buffer);
  dv.setUint32(total-8,Math.floor(bl/0x100000000)); dv.setUint32(total-4,bl>>>0);
  const w=new Uint32Array(64);
  for(let i=0;i<total;i+=64){ for(let t=0;t<16;t++)w[t]=dv.getUint32(i+t*4); for(let t=16;t<64;t++){ const s0=rotr(7,w[t-15])^rotr(18,w[t-15])^(w[t-15]>>>3),s1=rotr(17,w[t-2])^rotr(19,w[t-2])^(w[t-2]>>>10); w[t]=(w[t-16]+s0+w[t-7]+s1)>>>0; }
    let[a,b,c,d,e,f,g,h]=H;
    for(let t=0;t<64;t++){ const S1=rotr(6,e)^rotr(11,e)^rotr(25,e),ch=(e&f)^(~e&g),t1=(h+S1+ch+K[t]+w[t])>>>0,S0=rotr(2,a)^rotr(13,a)^rotr(22,a),maj=(a&b)^(a&c)^(b&c),t2=(S0+maj)>>>0; h=g;g=f;f=e;e=(d+t1)>>>0;d=c;c=b;b=a;a=(t1+t2)>>>0; }
    H=[(H[0]+a)>>>0,(H[1]+b)>>>0,(H[2]+c)>>>0,(H[3]+d)>>>0,(H[4]+e)>>>0,(H[5]+f)>>>0,(H[6]+g)>>>0,(H[7]+h)>>>0];
  }
  return H.map(x=>x.toString(16).padStart(8,"0")).join("");
}

/* ============================================================================
   UI + MQTT
   ==========================================================================*/
let client=null, txn=null, publishing=false, abortFlag=false;

/* ----------------------------------------------------------------------------
   Adaptacao nossa (nao mexe na logica do emissor): monta a lista ordenada de
   publicacoes {topic, payload} pronta para o backend publicar com QoS 1.
   O payload ja vai serializado por canon() - o backend publica a STRING como
   esta, sem reserializar, senao o hash declarado no begin nao bate.
---------------------------------------------------------------------------- */
function buildPublications(cfg, rid, opts) {
  const txn = buildTransaction(cfg, rid, opts || {});
  const pubs = [];
  pubs.push({ topic: txn.topics.request, payload: canon(txn.begin), kind: "begin" });
  txn.puts.forEach((p, i) => {
    pubs.push({
      topic: txn.topics.chunk,
      payload: p.raw,                    // raw ja e o canon() usado no hash
      kind: "put",
      entity: p.spec.entity,
      slot: p.spec.slot,
      sub: p.spec.sub,
      seq: i + 1,
    });
  });
  pubs.push({ topic: txn.topics.request, payload: canon(txn.validate), kind: "validate" });
  pubs.push({ topic: txn.topics.request, payload: canon(txn.commit), kind: "commit" });
  return {
    publications: pubs,
    topics: txn.topics,
    crc: txn.crc,
    entityCount: txn.specs.length,
    issues: txn.issues,               // entidades que estouraram 511B/96 tokens
  };
}

if (typeof module !== "undefined" && module.exports) {
  module.exports = { buildTransaction, buildPublications, canon, crc32, sha256Hex,
                     configurationEntitySpecs, makeMessage };
}
