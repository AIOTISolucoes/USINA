// =============================================================================
// gateway_specs.js — o "contrato" das telas do configurador do Gateway V16.
//
// PORTE FIEL do config_editor.py que o Igor mandou em 27/07 (publicador local):
// mesmas seções, mesmas chaves, mesmos rótulos, mesmos padrões. Quem manda no
// formato é o compilador dele (mqtt_entities.py, que roda no backend) — este
// arquivo só descreve como cada campo aparece na tela.
//
// ⚠️ NÃO invente chave nova aqui. Se um campo não existe no config_editor.py
// nem é lido pelo mqtt_entities.py, o gateway ignora e a tela mente.
//
// Acrescentado ao que ele tinha (ele mesmo apontou a falta em 27/07 23:51):
//   • Sequências (com o editor dos passos)
//   • PID (com o editor dos alvos)
//   • Alarmes e Eventos — campos tirados do próprio mqtt_entities.py
// =============================================================================

// tipo: "str" | "int" | "float" | "bool" | "json" | "lista"
// vals: sugestões (o campo continua aceitando texto livre, como o Combobox
//       editável do Tkinter — foi assim que "PSET"/"uint16_array" sobreviveram
//       na config real da Naturágua, que não estão na lista do editor dele)
// ref:  preenche as sugestões com os ids de outra seção
const GW_SECOES = [
  {
    key: "channels",
    titulo: "Comunicação",
    icone: "fa-network-wired",
    ajuda: "Um canal TCP representa um IP/porta e pode atender vários Unit IDs.",
    colunas: ["id", "name", "transport", "ip", "serial_port"],
    campos: [
      { k: "id", rot: "ID do canal" },
      { k: "name", rot: "Nome" },
      { k: "enabled", rot: "Habilitado", tipo: "bool", pad: true },
      { k: "transport", rot: "Transporte", vals: ["tcp", "rtu"], pad: "tcp" },
      { k: "ip", rot: "IP / host" },
      { k: "port", rot: "Porta TCP", tipo: "int", pad: 502 },
      { k: "serial_port", rot: "Porta serial", pad: "COM1" },
      { k: "baudrate", rot: "Baudrate", tipo: "int", pad: 9600 },
      { k: "data_bits", rot: "Data bits", tipo: "int", pad: 8 },
      { k: "parity", rot: "Paridade", vals: ["none", "even", "odd"], pad: "none" },
      { k: "stop_bits", rot: "Stop bits", tipo: "int", pad: 1 },
      { k: "timeout_ms", rot: "Timeout (ms)", tipo: "int", pad: 1000 },
      { k: "retries", rot: "Tentativas", tipo: "int", pad: 2 },
      { k: "poll_interval_ms", rot: "Ciclo do canal (ms)", tipo: "int", pad: 1000 },
      { k: "metadata.inter_device_ms", rot: "Intervalo entre Unit IDs (ms)", tipo: "int", pad: 10 },
      { k: "metadata.fast_skip_offline", rot: "Pular offline rapidamente", tipo: "bool", pad: true },
    ],
  },
  {
    key: "devices",
    titulo: "Devices",
    icone: "fa-microchip",
    ajuda: "Associa canal, Unit ID e template. O botão do catálogo cria vários de uma vez.",
    colunas: ["id", "name", "device_type", "channel_id", "unit_id", "template_id"],
    campos: [
      { k: "id", rot: "ID do device" },
      { k: "name", rot: "Nome" },
      { k: "description", rot: "Descrição" },
      { k: "enabled", rot: "Habilitado", tipo: "bool", pad: true },
      { k: "manufacturer", rot: "Fabricante" },
      { k: "model", rot: "Modelo" },
      {
        k: "device_type", rot: "Tipo",
        vals: ["inverter", "stringbox", "weather", "meter", "relay", "custom"],
      },
      { k: "channel_id", rot: "Canal", ref: "channels" },
      { k: "unit_id", rot: "Modbus Unit ID", tipo: "int", pad: 1 },
      { k: "template_id", rot: "Template", ref: "templates" },
      { k: "poll_interval_ms", rot: "Ciclo Modbus (ms)", tipo: "int", pad: 1000 },
      { k: "publish_interval_ms", rot: "Ciclo MQTT (ms)", tipo: "int", pad: 10000 },
      { k: "stale_timeout_ms", rot: "Tempo stale (ms)", tipo: "int", pad: 30000 },
      { k: "commands_enabled", rot: "Aceitar comandos", tipo: "bool", pad: false },
      { k: "metadata.domain", rot: "Empresa / domínio" },
      { k: "metadata.complex", rot: "Complexo", pad: "UFV" },
      { k: "metadata.power_plant", rot: "Usina" },
      { k: "metadata.energy_source", rot: "Fonte", pad: "Solar" },
      {
        k: "metadata.allow_unverified_map", rot: "Permitir mapa não verificado",
        tipo: "bool", pad: false,
        dica: "Template importado chega 'não validado em campo'. Sem isto o gateway recusa a transação INTEIRA.",
      },
    ],
  },
  {
    key: "templates",
    titulo: "Templates",
    icone: "fa-layer-group",
    ajuda: "Fabricante, modelo e tipo de equipamento. Requests e campos pendem daqui.",
    colunas: ["id", "name", "manufacturer", "model", "device_type", "version"],
    campos: [
      { k: "id", rot: "ID do template" },
      { k: "name", rot: "Nome" },
      { k: "enabled", rot: "Habilitado", tipo: "bool", pad: true },
      { k: "manufacturer", rot: "Fabricante" },
      { k: "model", rot: "Modelo" },
      { k: "device_type", rot: "Tipo de equipamento" },
      { k: "version", rot: "Versão", tipo: "int", pad: 1 },
      { k: "description", rot: "Descrição" },
      { k: "metadata.source", rot: "Fonte do mapa" },
      { k: "metadata.read_map", rot: "Resumo do mapa" },
    ],
  },
  {
    key: "requests",
    titulo: "Requests Modbus",
    icone: "fa-right-left",
    ajuda: "Até 32 blocos de leitura por template.",
    colunas: ["id", "template_id", "function_code", "address", "quantity", "buffer_offset"],
    campos: [
      { k: "id", rot: "ID da request" },
      { k: "template_id", rot: "Template", ref: "templates" },
      { k: "name", rot: "Nome" },
      { k: "enabled", rot: "Habilitada", tipo: "bool", pad: true },
      { k: "function_code", rot: "Function Code", tipo: "int", vals: ["1", "2", "3", "4"], pad: 3 },
      { k: "address", rot: "Endereço inicial", tipo: "int", pad: 0 },
      { k: "quantity", rot: "Quantidade", tipo: "int", pad: 1 },
      { k: "buffer_offset", rot: "Offset no buffer", tipo: "int", pad: 0 },
      { k: "interval_ms", rot: "Ciclo (ms)", tipo: "int", pad: 1000 },
      { k: "timeout_ms", rot: "Timeout (ms)", tipo: "int", pad: 1000 },
      { k: "retries", rot: "Tentativas", tipo: "int", pad: 2 },
      { k: "validation_status", rot: "Validação", vals: ["verified", "unverified"], pad: "unverified" },
      { k: "metadata.source", rot: "Documento de origem" },
    ],
  },
  {
    key: "fields",
    titulo: "Campos JSON",
    icone: "fa-code",
    ajuda: "Cada chave publicada: request, offset, tipo, ordem de words, ganho e status.",
    colunas: ["id", "template_id", "json_key", "request_id", "register_offset", "data_type"],
    campos: [
      { k: "id", rot: "ID do campo" },
      { k: "template_id", rot: "Template", ref: "templates" },
      { k: "request_id", rot: "Request", ref: "requests" },
      { k: "json_key", rot: "Chave no JSON" },
      { k: "enabled", rot: "Habilitado", tipo: "bool", pad: true },
      {
        k: "source_type", rot: "Origem",
        vals: ["modbus", "static", "derived", "timestamp", "quality"], pad: "modbus",
      },
      {
        k: "data_type", rot: "Tipo",
        vals: ["bool", "uint16", "int16", "uint32", "int32", "float32", "string"], pad: "uint16",
      },
      { k: "register_offset", rot: "Offset do registrador", tipo: "int", pad: 0 },
      { k: "bit_offset", rot: "Offset do bit", tipo: "int", pad: 0 },
      { k: "word_order", rot: "Ordem das words", vals: ["normal", "swapped"], pad: "normal" },
      { k: "byte_order", rot: "Ordem dos bytes", vals: ["big", "little"], pad: "big" },
      { k: "gain", rot: "Ganho", tipo: "float", pad: 1.0 },
      { k: "offset", rot: "Offset de engenharia", tipo: "float", pad: 0.0 },
      { k: "unit", rot: "Unidade" },
      { k: "default_value", rot: "Valor padrão" },
      { k: "source_expression", rot: "Expressão / valor derivado" },
      { k: "quality_source", rot: "Fonte da qualidade" },
      {
        k: "status_mapping", rot: "Correlação status JSON", tipo: "json", pad: {},
        dica: 'Ex.: {"0":0,"8192":1}. Máximo 16 correlações por template. ' +
              "A ORDEM do arquivo é preservada (o gateway parea na ordem).",
      },
      { k: "validation_status", rot: "Validação", vals: ["verified", "unverified"], pad: "unverified" },
      { k: "metadata.publish", rot: "Publicar", tipo: "bool", pad: true },
      { k: "metadata.decimals", rot: "Casas decimais", tipo: "int", pad: 3 },
      { k: "metadata.quote_value", rot: "Publicar entre aspas", tipo: "bool", pad: false },
    ],
  },
  {
    key: "topics",
    titulo: "Tópicos MQTT",
    icone: "fa-tower-broadcast",
    ajuda: "telemetry, command, feedback e ack. Use GERAR TÓPICOS para preencher pelo padrão.",
    colunas: ["id", "device_id", "purpose", "topic", "qos", "retain"],
    campos: [
      { k: "id", rot: "ID do tópico" },
      { k: "device_id", rot: "Device", ref: "devices" },
      {
        k: "purpose", rot: "Finalidade",
        vals: ["telemetry", "command", "feedback", "ack"], pad: "telemetry",
      },
      { k: "topic", rot: "Tópico" },
      { k: "qos", rot: "QoS", tipo: "int", vals: ["0", "1", "2"], pad: 1 },
      { k: "retain", rot: "Retain", tipo: "bool", pad: false },
      { k: "shared", rot: "Compartilhado", tipo: "bool", pad: false },
      { k: "editable", rot: "Editável", tipo: "bool", pad: true },
    ],
  },
  {
    key: "commands",
    titulo: "Escritas e comandos",
    icone: "fa-paper-plane",
    ajuda: "FC5, FC6, FC15 e FC16. Máximo 6 comandos por equipamento.",
    colunas: ["id", "device_id", "name", "function_code", "address", "fixed_value"],
    campos: [
      { k: "id", rot: "ID do comando" },
      { k: "device_id", rot: "Device", ref: "devices" },
      { k: "template_id", rot: "Template", ref: "templates" },
      {
        k: "name", rot: "Nome",
        vals: ["LIGA", "DESLIGA", "RESET", "SET1", "SET2", "SET3", "PSET", "PFSET", "TIME_SET", "RESET_ARC"],
      },
      { k: "enabled", rot: "Habilitado", tipo: "bool", pad: false },
      { k: "function_code", rot: "Function Code", tipo: "int", vals: ["5", "6", "15", "16"], pad: 6 },
      { k: "address", rot: "Endereço", tipo: "int", pad: 0 },
      {
        k: "data_type", rot: "Tipo",
        vals: ["bool", "uint16", "int16", "uint32", "float32", "uint16_array"], pad: "uint16",
      },
      { k: "fixed_value", rot: "Valor fixo/array JSON", tipo: "json", pad: 0 },
      { k: "use_payload", rot: "Usar valor do payload", tipo: "bool", pad: false },
      { k: "pulse_ms", rot: "Pulso (ms)", tipo: "int", pad: 0 },
      { k: "feedback_topic", rot: "Tópico de feedback" },
      { k: "metadata.min", rot: "Mínimo", tipo: "int", pad: 0 },
      { k: "metadata.max", rot: "Máximo", tipo: "int", pad: 65535 },
    ],
  },

  // ---------------------------------------------------------------------------
  // A PARTIR DAQUI: o que faltava no editor do Igor.
  // Campos lidos direto do mqtt_entities.py (configuration_entity_specs), que é
  // quem transforma isto em entidade do staging — não é chute.
  // ---------------------------------------------------------------------------
  {
    key: "sequences",
    titulo: "Sequências",
    icone: "fa-list-ol",
    ajuda: "Comando em vários passos (escrita, espera, pulso, verificação). Máximo 16 passos.",
    colunas: ["id", "name", "command_name", "device_id", "steps", "enabled"],
    colunasCalc: { steps: (r) => (Array.isArray(r.steps) ? r.steps.length : 0) },
    campos: [
      { k: "id", rot: "ID da sequência" },
      { k: "name", rot: "Nome" },
      { k: "enabled", rot: "Habilitada", tipo: "bool", pad: false },
      {
        k: "command_name", rot: "Comando que dispara",
        vals: ["LIGA", "DESLIGA", "RESET", "SET1", "SET2", "SET3"],
      },
      { k: "device_id", rot: "Device", ref: "devices" },
      { k: "max_retries", rot: "Tentativas", tipo: "int", pad: 0 },
      { k: "timeout_ms", rot: "Timeout total (ms)", tipo: "int", pad: 30000 },
      { k: "metadata.require_online", rot: "Exigir device online", tipo: "bool", pad: true },
      { k: "metadata.feedback_topic", rot: "Tópico de feedback" },
      {
        k: "steps", rot: "Passos", tipo: "lista", pad: [], max: 16,
        sub: {
          rotulo: "passo",
          colunas: ["type", "description", "function_code", "address", "value"],
          campos: [
            { k: "description", rot: "Descrição" },
            {
              k: "type", rot: "Operação",
              vals: ["write", "double_bit", "delay", "pulse", "verify", "condition"], pad: "write",
            },
            { k: "function_code", rot: "Function Code", tipo: "int", vals: ["5", "6", "15", "16"], pad: 6 },
            { k: "address", rot: "Endereço", tipo: "int", pad: 0 },
            { k: "values", rot: "Valores (array JSON)", tipo: "json", pad: [0] },
            { k: "delay_ms", rot: "Espera (ms)", tipo: "int", pad: 0 },
            { k: "pulse_ms", rot: "Pulso (ms)", tipo: "int", pad: 0 },
            { k: "retries", rot: "Tentativas", tipo: "int", pad: 0 },
            { k: "field_id", rot: "Campo de confirmação", ref: "fields" },
            { k: "confirm_function_code", rot: "FC de confirmação", tipo: "int", vals: ["1", "2", "3", "4"], pad: 3 },
            { k: "confirm_address", rot: "Endereço de confirmação", tipo: "int", pad: 0 },
            { k: "confirm_quantity", rot: "Quantidade de confirmação", tipo: "int", pad: 1 },
            {
              k: "operator", rot: "Operador",
              vals: ["==", "!=", ">", ">=", "<", "<=", "bit_set", "bit_clear", "between", "outside"],
              pad: "==",
            },
            { k: "value", rot: "Valor esperado", tipo: "int", pad: 0 },
            { k: "mask", rot: "Máscara", tipo: "int", pad: 0 },
            { k: "expected_high", rot: "Limite superior", tipo: "int", pad: 0 },
            { k: "next_success", rot: "Próximo passo se OK", tipo: "int", pad: 0 },
            { k: "next_failure", rot: "Próximo passo se falhar", tipo: "int", pad: 0 },
          ],
        },
      },
    ],
  },
  {
    key: "pid",
    titulo: "PID",
    icone: "fa-sliders",
    ajuda: "Controle de potência por realimentação. Máximo 16 controladores.",
    colunas: ["id", "name", "process_field_id", "setpoint", "mode", "enabled"],
    campos: [
      { k: "id", rot: "ID do PID" },
      { k: "name", rot: "Nome" },
      { k: "enabled", rot: "Habilitado", tipo: "bool", pad: false },
      {
        k: "process_field_id", rot: "Variável de processo (campo)", ref: "fields",
        dica: "Campo JSON que o PID lê como PV (ex.: potência ativa do medidor).",
      },
      {
        k: "metadata.pv_device_id", rot: "Device da variável", ref: "devices",
        dica: "Se vazio, o compilador deduz pelo template do campo.",
      },
      { k: "setpoint", rot: "Setpoint", tipo: "float", pad: 0 },
      { k: "kp", rot: "Kp", tipo: "float", pad: 0 },
      { k: "ki", rot: "Ki", tipo: "float", pad: 0 },
      { k: "kd", rot: "Kd", tipo: "float", pad: 0 },
      { k: "sample_ms", rot: "Amostragem (ms)", tipo: "int", pad: 1000 },
      { k: "output_min", rot: "Saída mínima", tipo: "float", pad: 0 },
      { k: "output_max", rot: "Saída máxima", tipo: "float", pad: 100 },
      { k: "ramp_up", rot: "Rampa de subida", tipo: "float", pad: 0 },
      { k: "ramp_down", rot: "Rampa de descida", tipo: "float", pad: 0 },
      { k: "deadband", rot: "Banda morta", tipo: "float", pad: 0 },
      { k: "mode", rot: "Modo", vals: ["manual", "auto"], pad: "manual" },
      { k: "metadata.manual_output", rot: "Saída em manual", tipo: "float", pad: 0 },
      { k: "safe_output", rot: "Saída segura", tipo: "float", pad: 0 },
      { k: "stale_timeout_ms", rot: "Tempo stale (ms)", tipo: "int", pad: 30000 },
      {
        k: "metadata.targets", rot: "Alvos", tipo: "lista", pad: [], max: 32,
        sub: {
          rotulo: "alvo",
          colunas: ["device_id", "nominal_power_kw", "minimum_percent", "maximum_percent"],
          campos: [
            { k: "device_id", rot: "Device", ref: "devices" },
            { k: "enabled", rot: "Habilitado", tipo: "bool", pad: true },
            { k: "nominal_power_kw", rot: "Potência nominal (kW)", tipo: "float", pad: 0 },
            { k: "minimum_percent", rot: "Mínimo (%)", tipo: "float", pad: 0 },
            { k: "maximum_percent", rot: "Máximo (%)", tipo: "float", pad: 100 },
            { k: "sequence_id", rot: "Sequência", ref: "sequences" },
            { k: "command_index", rot: "Índice do comando", tipo: "int", pad: 0 },
          ],
        },
      },
    ],
  },
  {
    key: "alarms",
    titulo: "Alarmes",
    icone: "fa-triangle-exclamation",
    ajuda: "Comparação de um campo com um limite, com atraso e severidade.",
    colunas: ["id", "name", "device_id", "field_id", "condition", "threshold", "severity"],
    campos: [
      { k: "id", rot: "ID do alarme" },
      { k: "name", rot: "Nome" },
      { k: "enabled", rot: "Habilitado", tipo: "bool", pad: true },
      { k: "device_id", rot: "Device", ref: "devices" },
      { k: "field_id", rot: "Campo (chave JSON)" },
      {
        k: "condition", rot: "Condição",
        vals: ["==", "!=", ">", ">=", "<", "<=", "bit_set", "bit_clear", "between", "outside"],
        pad: "==",
      },
      { k: "threshold", rot: "Limite", tipo: "float", pad: 0 },
      { k: "metadata.threshold_b", rot: "Limite B (between/outside)", tipo: "float", pad: 0 },
      { k: "metadata.hysteresis", rot: "Histerese", tipo: "float", pad: 0 },
      { k: "delay_ms", rot: "Atraso para ligar (ms)", tipo: "int", pad: 0 },
      { k: "metadata.delay_off_ms", rot: "Atraso para desligar (ms)", tipo: "int", pad: 0 },
      { k: "severity", rot: "Severidade", vals: ["info", "warning", "alarm", "critical"], pad: "warning" },
      { k: "metadata.latch", rot: "Trava até reconhecer", tipo: "bool", pad: false },
      { k: "message", rot: "Mensagem" },
      { k: "metadata.topic", rot: "Tópico" },
    ],
  },
  {
    key: "events",
    titulo: "Eventos",
    icone: "fa-bell",
    ajuda: "Publica uma mensagem quando um campo casa com a condição.",
    colunas: ["id", "name", "device_id", "field_id", "condition", "metadata.event_code"],
    campos: [
      { k: "id", rot: "ID do evento" },
      { k: "name", rot: "Nome" },
      { k: "enabled", rot: "Habilitado", tipo: "bool", pad: true },
      { k: "device_id", rot: "Device", ref: "devices" },
      { k: "field_id", rot: "Campo (chave JSON)" },
      {
        k: "condition", rot: "Condição",
        vals: ["==", "!=", ">", ">=", "<", "<=", "bit_set", "bit_clear", "between", "outside"],
        pad: "==",
      },
      { k: "metadata.expected_a", rot: "Valor esperado", tipo: "float", pad: 0 },
      { k: "metadata.expected_b", rot: "Valor esperado B", tipo: "float", pad: 0 },
      { k: "metadata.event_code", rot: "Código do evento", tipo: "int", pad: 0 },
      { k: "message", rot: "Mensagem" },
      { k: "metadata.topic", rot: "Tópico" },
      { k: "metadata.qos", rot: "QoS", tipo: "int", vals: ["0", "1", "2"], pad: 1 },
      { k: "metadata.retain", rot: "Retain", tipo: "bool", pad: false },
    ],
  },
];

// Aba "Usina e gateway" — porte fiel do GENERAL_FIELDS do config_editor.py.
const GW_CAMPOS_GERAIS = [
  { k: "plant.id", rot: "ID da usina", pad: "NovaUsina",
    dica: "É o segmento do tópico. Precisa bater EXATO com o nome da usina no cadastro (a ingestão casa case-sensitive)." },
  { k: "plant.name", rot: "Nome da usina" },
  { k: "plant.company", rot: "Empresa" },
  { k: "plant.client", rot: "Cliente" },
  { k: "plant.timezone", rot: "Fuso horário", pad: "America/Fortaleza" },
  { k: "plant.metadata.complex", rot: "Complexo", pad: "UFV" },
  { k: "plant.metadata.topic_slug", rot: "Nome da usina nos tópicos" },
  { k: "general.gateway_id", rot: "ID do gateway" },
  { k: "general.quality_good", rot: "Qualidade boa", tipo: "int", pad: 192 },
  { k: "general.quality_bad", rot: "Qualidade ruim", tipo: "int", pad: 28 },
  { k: "general.default_qos", rot: "QoS padrão", tipo: "int", vals: ["0", "1", "2"], pad: 1 },
  { k: "general.default_retain", rot: "Retain padrão", tipo: "bool", pad: false },
  { k: "general.scada_server_enabled", rot: "Servidor Modbus TCP", tipo: "bool", pad: true },
  { k: "general.scada_port", rot: "Porta SCADA", tipo: "int", pad: 502 },
  { k: "general.scada_unit_id", rot: "Unit ID SCADA", tipo: "int", pad: 255 },
  { k: "general.metadata.scada_interface", rot: "Interface/IP SCADA" },
  { k: "general.metadata.auto_topics", rot: "Gerar tópicos automaticamente", tipo: "bool", pad: true },
  { k: "general.metadata.telemetry_pattern", rot: "Padrão telemetry", pad: "dev/read/UFV/{plant}/{type}/{id}" },
  { k: "general.metadata.command_pattern", rot: "Padrão command", pad: "dev/write/UFV/{plant}/{type}/{id}" },
  { k: "general.command_subscribe_filter", rot: "Filtro de comandos" },
  { k: "general.command_feedback_topic", rot: "Feedback de comandos" },
  { k: "general.config_command_topic", rot: "Tópico configuração" },
  { k: "general.config_feedback_topic", rot: "Feedback configuração" },
];

// Capacidades do staging do CC100 (tabela "Capacidades" da doc do V16).
// Estourar qualquer uma faz o validate recusar a TRANSAÇÃO INTEIRA — e o
// sintoma é "publicou e não aplicou", sem erro nenhum na tela.
const GW_LIMITES = {
  channels: 32, templates: 100, devices: 255, sequences: 128, pid: 16,
  requests_por_template: 32, fields_por_template: 192, commands_por_device: 6,
  status_map_por_template: 16, passos_por_sequencia: 16, alvos_por_pid: 32,
  reclose_rules: 32,
};

// Config nova — mesmo new_configuration() do config_editor.py dele.
function gwNovaConfiguracao(usina) {
  const nome = String(usina || "NovaUsina").trim() || "NovaUsina";
  return {
    format: "aioti.gateway.configuration",
    schema_version: "1.0.0",
    configuration_id: nome.toLowerCase(),
    revision: 1,
    plant: {
      id: nome, name: nome, company: "", client: "",
      timezone: "America/Fortaleza",
      metadata: { complex: "UFV", topic_slug: nome },
    },
    general: {
      timezone: "America/Fortaleza", quality_good: 192, quality_bad: 28,
      default_qos: 1, default_retain: false,
      scada_server_enabled: true, scada_port: 502, scada_unit_id: 255,
      gateway_id: "GW-" + nome.toUpperCase().slice(0, 12),
      plant_id: nome,
      command_subscribe_filter: `dev/write/UFV/${nome}/+/+`,
      command_feedback_topic: `dev/write/UFV/${nome}/feedback`,
      config_command_topic: `dev/write/UFV/${nome}/gateway/configuration`,
      config_feedback_topic: `dev/write/UFV/${nome}/gateway/configuration/feedback`,
      metadata: {
        auto_topics: true,
        telemetry_pattern: "dev/read/UFV/{plant}/{type}/{id}",
        command_pattern: "dev/write/UFV/{plant}/{type}/{id}",
        scada_interface: "",
      },
    },
    channels: [], devices: [], templates: [], requests: [], fields: [],
    topics: [], alarms: [], events: [], commands: [], sequences: [], pid: [],
    auto_reclosing: {
      enabled: false, allowed_protections: [], blocking_conditions: [], max_attempts: 0,
    },
  };
}
