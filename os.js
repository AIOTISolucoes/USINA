// =============================================================================
// OS.JS — AIOTI Kanban v2
// =============================================================================

const COLUMNS = [
  { status:'pendente',       color:'#f59e0b' },
  { status:'em_processo',    color:'#3b82f6' },
  { status:'em_verificacao', color:'#a855f7' },
  { status:'concluida',      color:'#2aff7b' },
];

const KB = {
  currentOs: null, currentStep: 1,
  assetFilters: {}, assetSearch: '', selectedAsset: null,
  subtasks: [], resources: [],
  failedChecked: false, serviceChecked: false, alreadyDoneChecked: false,
  selectedResponsavel: null,
  _searchTimer: null, _assetTimer: null, _respTimer: null, _autoRefresh: null,
  _statusTarget: null,
  respUsers: [], // loaded once
};

// ─── INIT ─────────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  initUser();
  initTheme();
  bindTopbar();
  bindFab();
  bindWizard();
  bindAssetDrawer();
  bindRespDrawer();
  bindDetailDrawer();
  loadAllColumns();
  KB._autoRefresh = setInterval(() => loadAllColumns({ silent:true }), 45000);
});

// ─── USER / THEME ─────────────────────────────────────────────────────────────
function initUser() {
  const u = JSON.parse(localStorage.getItem('user') || '{}');
  const name = u.name || u.username || '?';
  const initials = name.split(' ').map(w=>w[0]).join('').slice(0,2).toUpperCase();
  document.getElementById('userAvatar').textContent = initials;
  document.getElementById('userNameDisplay').textContent = name;
  document.getElementById('f-requested-by').value = name;
}

function initTheme() {
  const saved = localStorage.getItem('theme') || 'dark';
  document.body.className = `theme-${saved}`;
  updateThemeIcon(saved);
  document.getElementById('themeToggleBtn')?.addEventListener('click', () => {
    const next = document.body.classList.contains('theme-dark') ? 'light' : 'dark';
    document.body.className = `theme-${next}`;
    localStorage.setItem('theme', next);
    updateThemeIcon(next);
  });
  document.getElementById('logoutBtn')?.addEventListener('click', () => {
    if (typeof logout === 'function') logout();
    else { localStorage.clear(); window.location='index.html'; }
  });
}
function updateThemeIcon(t){ const i=document.getElementById('themeIcon'); if(i) i.className=t==='dark'?'fa-solid fa-moon':'fa-solid fa-sun'; }

// ─── TOPBAR ───────────────────────────────────────────────────────────────────
function bindTopbar() {
  document.getElementById('kbSearchInput')?.addEventListener('input', () => {
    clearTimeout(KB._searchTimer);
    KB._searchTimer = setTimeout(() => loadAllColumns(), 400);
  });
  document.getElementById('kbRefreshBtn')?.addEventListener('click', () => loadAllColumns());
  document.querySelectorAll('.kb-col-refresh').forEach(btn =>
    btn.addEventListener('click', () => loadColumn(btn.dataset.col))
  );
}

// ─── LOAD KANBAN ──────────────────────────────────────────────────────────────
async function loadAllColumns({ silent=false }={}) {
  const q = document.getElementById('kbSearchInput')?.value?.trim() || '';
  await Promise.all(COLUMNS.map(c => loadColumn(c.status, { silent, q })));
}

async function loadColumn(status, { silent=false, q='' }={}) {
  const col     = document.getElementById(`col-${status}`);
  const loading = document.getElementById(`loading-${status}`);
  const countEl = document.getElementById(`count-${status}`);
  if (!col) return;
  if (!silent) { loading?.classList.remove('hidden'); col.innerHTML=''; col.appendChild(loading); }
  const params = new URLSearchParams({ status });
  if (q) params.set('q', q);
  try {
    const res = await apiFetch(`/work-orders?${params}`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    const items = data.items || [];
    if (countEl) countEl.textContent = items.length;
    renderColumn(status, items);
  } catch(e) {
    console.error('[KB] load column', status, e);
    if (!silent) col.innerHTML = `<div class="kb-empty"><i class="fa-solid fa-triangle-exclamation"></i><span>Erro ao carregar</span></div>`;
  } finally { loading?.classList.add('hidden'); }
}

function renderColumn(status, items) {
  const col = document.getElementById(`col-${status}`);
  if (!col) return;
  col.innerHTML = '';
  if (!items.length) {
    col.innerHTML = `<div class="kb-empty"><i class="fa-regular fa-rectangle-list"></i><span>Nenhuma OS aqui</span></div>`;
    return;
  }
  items.forEach(os => col.appendChild(buildCard(os)));
}

// ─── CARD ─────────────────────────────────────────────────────────────────────
function buildCard(os) {
  const card = document.createElement('div');
  card.className = 'kb-card';
  const accent = { pendente:'#f59e0b', em_processo:'#3b82f6', em_verificacao:'#a855f7', concluida:'#2aff7b', cancelada:'#ef4444' };
  card.style.setProperty('--card-accent', accent[os.status]||'transparent');
  const pct = os.progress ?? 0;
  const initials = avatarInitials(os.assignee_name || os.requested_by || '?');
  card.innerHTML = `
    ${os.status==='cancelada'?'<div class="kb-cancelled-badge">Cancelado</div>':''}
    <div class="kb-card-header">
      <div class="kb-card-num">${os.os_number||os.id}</div>
      <div class="kb-card-creator">Criado por ${esc(os.created_by||os.requested_by||'—')}</div>
    </div>
    ${os.asset_name?`<div class="kb-card-asset"><strong>Ativo:</strong> ${esc(os.asset_name)}${os.asset_location?` <span style="font-size:10px;opacity:.6;">( ${esc(os.asset_location)} )</span>`:''}</div>`:''}
    ${os.task_description?`<div class="kb-card-task"><strong>Tarefa:</strong> ${esc(os.task_description)}</div>`:''}
    <div class="kb-card-progress">
      <div class="kb-progress-bar"><div class="kb-progress-fill" style="width:${pct}%"></div></div>
      <div class="kb-progress-pct">${pct} %</div>
    </div>
    <div class="kb-card-footer">
      <div class="kb-card-meta">
        <div class="kb-meta-item"><i class="fa-regular fa-clock"></i>${esc(os.estimated_duration||'—')}</div>
        <div class="kb-meta-item ${isOverdue(os)?'overdue':''}"><i class="fa-regular fa-calendar"></i>${fmtDate(os.incident_date)}</div>
        ${os.asset_failed?'<div class="kb-failed-badge"><i class="fa-solid fa-triangle-exclamation"></i>Falha</div>':''}
      </div>
      <div class="kb-card-actions">
        <div class="kb-assignee-av" title="${esc(os.assignee_name||os.requested_by||'?')}">${initials}</div>
        <button class="kb-card-action-btn" data-action="share"><i class="fa-solid fa-share-nodes"></i></button>
        <button class="kb-card-action-btn" data-action="menu"><i class="fa-solid fa-ellipsis-vertical"></i></button>
      </div>
    </div>`;
  card.addEventListener('click', e => { if (e.target.closest('[data-action]')) return; openDetail(os); });
  card.querySelector('[data-action="share"]')?.addEventListener('click', e => {
    e.stopPropagation();
    navigator.clipboard?.writeText(`OS #${os.os_number} — ${os.task_description||os.asset_name}`).catch(()=>{});
    showToast('Copiado!','success');
  });
  card.querySelector('[data-action="menu"]')?.addEventListener('click', e => {
    e.stopPropagation(); openStatusPopover(e.currentTarget, os);
  });
  return card;
}

function isOverdue(os) {
  if (!os.scheduled_date || os.status==='concluida') return false;
  return new Date(os.scheduled_date) < new Date();
}
function avatarInitials(name) {
  return (name||'?').split(' ').map(w=>w[0]).join('').slice(0,2).toUpperCase();
}

// ─── STATUS POPOVER ───────────────────────────────────────────────────────────
function openStatusPopover(anchor, os) {
  const pop = document.getElementById('statusPopover');
  const rect = anchor.getBoundingClientRect();
  pop.style.top  = `${rect.bottom+6}px`;
  pop.style.right = `${window.innerWidth-rect.right}px`;
  pop.classList.remove('hidden');
  KB._statusTarget = os;
  const hide = e => { if (!pop.contains(e.target)){ pop.classList.add('hidden'); document.removeEventListener('click',hide,true); } };
  setTimeout(() => document.addEventListener('click',hide,true), 10);
}

document.getElementById('statusPopover')?.addEventListener('click', async e => {
  const btn = e.target.closest('.sp-btn');
  if (!btn||!KB._statusTarget) return;
  document.getElementById('statusPopover').classList.add('hidden');
  await changeStatus(KB._statusTarget.id, btn.dataset.status);
});

async function changeStatus(id, status) {
  try {
    const res = await apiFetch(`/work-orders/${id}/status`, {
      method:'PATCH', headers:{'Content-Type':'application/json'}, body:JSON.stringify({status}),
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    showToast('Status atualizado!','success');
    await loadAllColumns({ silent:true });
  } catch(e) { showToast('Erro ao atualizar status','error'); }
}

// ─── DETAIL DRAWER ────────────────────────────────────────────────────────────
function bindDetailDrawer() {
  document.getElementById('osdCloseBtn')?.addEventListener('click', closeDetail);
  document.getElementById('osDetailOverlay')?.addEventListener('click', closeDetail);
  document.getElementById('osdStatusBtn')?.addEventListener('click', e => {
    if (KB.currentOs) openStatusPopover(e.currentTarget, KB.currentOs);
  });
}
function openDetail(os) {
  KB.currentOs = os;
  document.getElementById('osdNum').textContent = `#${os.os_number||os.id}`;
  const statusLabels = { pendente:'Pendente', em_processo:'Em Processo', em_verificacao:'Em Verificação', concluida:'Concluída', cancelada:'Cancelada' };
  const body = document.getElementById('osdBody');
  body.innerHTML = `
    <div class="osd-section">
      <div class="osd-section-title">Status</div>
      <span class="osd-status-pill ${os.status}">${statusLabels[os.status]||os.status}</span>
    </div>
    <div class="osd-section">
      <div class="osd-section-title">Informações</div>
      <div class="osd-info-grid">
        <div class="osd-info-item"><span class="osd-info-label">Ativo</span><span class="osd-info-value">${esc(os.asset_name||'—')}</span></div>
        <div class="osd-info-item"><span class="osd-info-label">Requerido por</span><span class="osd-info-value">${esc(os.requested_by||'—')}</span></div>
        <div class="osd-info-item"><span class="osd-info-label">Data Incidente</span><span class="osd-info-value">${fmtDatetime(os.incident_date)}</span></div>
        <div class="osd-info-item"><span class="osd-info-label">Tipo de Tarefa</span><span class="osd-info-value">${esc(os.task_type||'—')}</span></div>
        <div class="osd-info-item"><span class="osd-info-label">Classificação 1</span><span class="osd-info-value">${esc(os.classification_1||'—')}</span></div>
        <div class="osd-info-item"><span class="osd-info-label">Classificação 2</span><span class="osd-info-value">${esc(os.classification_2||'—')}</span></div>
        <div class="osd-info-item"><span class="osd-info-label">Criticidade</span><span class="osd-info-value">${esc(os.criticality||'—')}</span></div>
        <div class="osd-info-item"><span class="osd-info-label">Duração est.</span><span class="osd-info-value">${esc(os.estimated_duration||'—')}</span></div>
        ${os.responsavel_name?`<div class="osd-info-item"><span class="osd-info-label">Responsável</span><span class="osd-info-value">${esc(os.responsavel_name)}</span></div>`:''}
      </div>
    </div>
    ${os.task_description?`<div class="osd-section"><div class="osd-section-title">Descrição</div><div class="osd-info-value" style="font-weight:400;line-height:1.5;font-size:13px;">${esc(os.task_description)}</div></div>`:''}
    ${os.observations?`<div class="osd-section"><div class="osd-section-title">Observações</div><div class="osd-info-value" style="font-weight:400;line-height:1.5;font-size:13px;">${esc(os.observations)}</div></div>`:''}
    ${os.asset_failed&&os.failure_type?`
    <div class="osd-section">
      <div class="osd-section-title">Dados da Falha</div>
      <div class="osd-info-grid">
        <div class="osd-info-item"><span class="osd-info-label">Tipo</span><span class="osd-info-value">${esc(os.failure_type)}</span></div>
        <div class="osd-info-item"><span class="osd-info-label">Causa</span><span class="osd-info-value">${esc(os.failure_cause||'—')}</span></div>
        <div class="osd-info-item"><span class="osd-info-label">Detecção</span><span class="osd-info-value">${esc(os.failure_detection_method||'—')}</span></div>
        <div class="osd-info-item"><span class="osd-info-label">Severidade</span><span class="osd-info-value">${esc(os.failure_severity||'—')}</span></div>
      </div>
    </div>`:''}`;
  document.getElementById('osDetailOverlay').classList.remove('hidden');
  document.getElementById('osDetailDrawer').classList.remove('hidden');
}
function closeDetail() {
  document.getElementById('osDetailOverlay').classList.add('hidden');
  document.getElementById('osDetailDrawer').classList.add('hidden');
  KB.currentOs = null;
}

// ─── FAB ─────────────────────────────────────────────────────────────────────
function bindFab() {
  document.getElementById('kbFab')?.addEventListener('click', openWizard);
}

// ─── WIZARD ──────────────────────────────────────────────────────────────────
function bindWizard() {
  document.getElementById('wzCloseBtn')?.addEventListener('click', closeWizard);
  document.getElementById('wzCancelBtn')?.addEventListener('click', closeWizard);
  document.getElementById('wzBackBtn')?.addEventListener('click', () => goToStep(KB.currentStep-1));
  document.getElementById('wzPrevBtn')?.addEventListener('click', () => goToStep(KB.currentStep-1));
  document.getElementById('wzNextBtn')?.addEventListener('click', () => goToStep(KB.currentStep+1));
  document.getElementById('wzSubmitBtn')?.addEventListener('click', submitWizard);

  // "O ativo falhou?" checkbox
  document.getElementById('failedCheckLabel')?.addEventListener('click', () => {
    KB.failedChecked = !KB.failedChecked;
    document.getElementById('failedCheck')?.classList.toggle('checked', KB.failedChecked);
    document.getElementById('failureFields')?.classList.toggle('hidden', !KB.failedChecked);
  });

  // "Ativo volta ao serviço?" checkbox
  document.getElementById('serviceCheckLabel')?.addEventListener('click', () => {
    KB.serviceChecked = !KB.serviceChecked;
    document.getElementById('serviceCheck')?.classList.toggle('checked', KB.serviceChecked);
  });

  // "Esta tarefa já foi realizada?" — toggle radio groups + responsável
  document.getElementById('alreadyDoneLabel')?.addEventListener('click', () => {
    KB.alreadyDoneChecked = !KB.alreadyDoneChecked;
    document.getElementById('alreadyDoneCheck')?.classList.toggle('checked', KB.alreadyDoneChecked);
    document.getElementById('radioGroupNormal')?.classList.toggle('hidden', KB.alreadyDoneChecked);
    document.getElementById('radioGroupDone')?.classList.toggle('hidden', !KB.alreadyDoneChecked);
    document.getElementById('responsavelGroup')?.classList.toggle('hidden', !KB.alreadyDoneChecked);
  });

  // Subtasks & resources
  document.getElementById('addSubtaskBtn')?.addEventListener('click', addSubtask);
  document.getElementById('addResourceBtn')?.addEventListener('click', addResource);
}

function openWizard() {
  resetWizard();
  document.getElementById('wzOverlay').classList.remove('hidden');
  const now = new Date();
  const local = new Date(now.getTime()-now.getTimezoneOffset()*60000).toISOString().slice(0,16);
  document.getElementById('f-incident-date').value = local;
  document.getElementById('f-scheduled-date').value = local;
  document.getElementById('f-start-date').value = local;
}

function closeWizard() {
  document.getElementById('wzOverlay').classList.add('hidden');
}

function resetWizard() {
  KB.currentStep=1; KB.selectedAsset=null; KB.selectedResponsavel=null;
  KB.subtasks=[]; KB.resources=[];
  KB.failedChecked=false; KB.serviceChecked=false; KB.alreadyDoneChecked=false;

  ['f-task-desc','f-observation','f-request-num'].forEach(id=>{ const el=document.getElementById(id); if(el) el.value=''; });
  document.getElementById('f-duration').value='000:10';
  document.getElementById('f-interruption-duration').value='000:00';
  ['f-failure-type','f-failure-cause','f-detection-method','f-failure-severity','f-task-type','f-class1','f-class2'].forEach(id=>{ const el=document.getElementById(id); if(el) el.value=''; });
  document.getElementById('f-criticality').value='media';
  document.getElementById('f-damage-type').value='Nenhum';

  ['failedCheck','serviceCheck','alreadyDoneCheck'].forEach(id=>document.getElementById(id)?.classList.remove('checked'));
  document.getElementById('failureFields')?.classList.add('hidden');
  document.getElementById('radioGroupNormal')?.classList.remove('hidden');
  document.getElementById('radioGroupDone')?.classList.add('hidden');
  document.getElementById('responsavelGroup')?.classList.add('hidden');

  const assetSel = document.getElementById('assetSelected');
  const assetBtn = document.getElementById('assetSearchBtn');
  assetSel?.classList.add('hidden');
  assetBtn?.classList.remove('hidden');

  const respLabel = document.getElementById('responsavelBtnLabel');
  if (respLabel) respLabel.textContent = 'Selecionar responsável...';

  document.getElementById('sendToPending').checked = true;
  document.getElementById('subtasksList').innerHTML = '<div class="wz-subtasks-empty"><i class="fa-regular fa-circle-check"></i><span>Nenhuma subtarefa adicionada</span></div>';
  document.getElementById('resourcesList').innerHTML = '<div class="wz-subtasks-empty"><i class="fa-regular fa-toolbox"></i><span>Nenhum recurso adicionado</span></div>';
  document.querySelectorAll('.wz-error').forEach(el=>el.classList.add('hidden'));

  goToStep(1, true);
}

function goToStep(step, force=false) {
  if (step<1||step>4) return;
  if (!force && step>KB.currentStep && !validateStep(KB.currentStep)) return;
  KB.currentStep = step;

  for (let i=1;i<=4;i++) document.getElementById(`wz-step-${i}`)?.classList.toggle('hidden', i!==step);

  document.querySelectorAll('.wz-step[data-step]').forEach(el=>{
    const n=parseInt(el.dataset.step);
    el.classList.toggle('active',n===step);
    el.classList.toggle('done',n<step);
  });

  document.getElementById('wzBackBtn')?.classList.toggle('hidden', step<=1);
  document.getElementById('wzPrevBtn')?.classList.toggle('hidden', step<=1);
  document.getElementById('wzNextBtn')?.classList.toggle('hidden', step>=4);
  document.getElementById('wzSubmitBtn')?.classList.toggle('hidden', step<4);

  if (step===4) buildSummary();
}

function validateStep(step) {
  let ok=true;
  if (step===1) {
    if (!KB.selectedAsset) {
      document.getElementById('err-asset')?.classList.remove('hidden');
      document.getElementById('assetSearchBtn')?.classList.add('has-error');
      ok=false;
    }
    if (KB.failedChecked) {
      ['f-failure-type','f-failure-cause','f-detection-method'].forEach(id=>{
        const el=document.getElementById(id);
        if (!el?.value) { document.getElementById(id.replace('f-','err-'))?.classList.remove('hidden'); ok=false; }
      });
    }
  }
  if (step===2) {
    const desc=document.getElementById('f-task-desc')?.value?.trim();
    if (!desc) { document.getElementById('err-task-desc')?.classList.remove('hidden'); ok=false; }
    const type=document.getElementById('f-task-type')?.value;
    if (!type) { document.getElementById('err-task-type')?.classList.remove('hidden'); ok=false; }
    if (KB.alreadyDoneChecked && !KB.selectedResponsavel) {
      document.getElementById('err-responsavel')?.classList.remove('hidden'); ok=false;
    }
  }
  return ok;
}

function buildSummary() {
  const grid=document.getElementById('wzSummaryGrid');
  if (!grid) return;
  const sendTo = KB.alreadyDoneChecked
    ? (document.getElementById('sendToVerif')?.checked ? 'Verificação' : 'Finalizados')
    : (document.getElementById('sendToPending')?.checked ? 'Tarefas pendentes' : 'OSs em Processo');
  const items=[
    ['Ativo', KB.selectedAsset?.name||'—'],
    ['Código', KB.selectedAsset?.code||'—'],
    ['Incidente', document.getElementById('f-incident-date')?.value||'—'],
    ['Ativo Falhou?', KB.failedChecked?'Sim':'Não'],
    ['Tipo Tarefa', document.getElementById('f-task-type')?.value||'—'],
    ['Classificação 1', document.getElementById('f-class1')?.value||'—'],
    ['Classificação 2', document.getElementById('f-class2')?.value||'—'],
    ['Responsável', KB.selectedResponsavel?.name||'—'],
    ['Enviar para', sendTo],
    ['Subtarefas', KB.subtasks.filter(Boolean).length.toString()],
  ];
  grid.innerHTML=items.map(([k,v])=>`<div class="wz-summary-item"><span class="wz-summary-key">${k}</span><span class="wz-summary-val">${esc(v)}</span></div>`).join('');
}

async function submitWizard() {
  if (!validateStep(1)||!validateStep(2)) { showToast('Preencha os campos obrigatórios','error'); return; }

  const btn=document.getElementById('wzSubmitBtn');
  btn.disabled=true; btn.innerHTML='<i class="fa-solid fa-spinner fa-spin"></i> Gerando...';

  let sendStatus;
  if (KB.alreadyDoneChecked) {
    sendStatus = document.getElementById('sendToVerif')?.checked ? 'em_verificacao' : 'concluida';
  } else {
    sendStatus = document.getElementById('sendToPending')?.checked ? 'pendente' : 'em_processo';
  }

  const body={
    asset_id: KB.selectedAsset?.id||null,
    asset_name: KB.selectedAsset?.name||null,
    asset_code: KB.selectedAsset?.code||null,
    asset_location: KB.selectedAsset?.location||null,
    plant_id: KB.selectedAsset?.plant_id||null,
    incident_date: document.getElementById('f-incident-date')?.value||new Date().toISOString(),
    requested_by: document.getElementById('f-requested-by')?.value||null,
    asset_failed: KB.failedChecked,
    failure_type: KB.failedChecked?document.getElementById('f-failure-type')?.value:null,
    failure_cause: KB.failedChecked?document.getElementById('f-failure-cause')?.value:null,
    failure_detection_method: KB.failedChecked?document.getElementById('f-detection-method')?.value:null,
    failure_severity: KB.failedChecked?document.getElementById('f-failure-severity')?.value:null,
    damage_type: KB.failedChecked?document.getElementById('f-damage-type')?.value:'Nenhum',
    caused_interruption_duration: KB.failedChecked?document.getElementById('f-interruption-duration')?.value:'000:00',
    back_to_service: KB.serviceChecked,
    task_description: document.getElementById('f-task-desc')?.value?.trim(),
    observations: document.getElementById('f-observation')?.value?.trim()||null,
    task_type: document.getElementById('f-task-type')?.value,
    classification_1: document.getElementById('f-class1')?.value||null,
    classification_2: document.getElementById('f-class2')?.value||null,
    criticality: document.getElementById('f-criticality')?.value||'media',
    estimated_duration: document.getElementById('f-duration')?.value||'000:10',
    request_number: document.getElementById('f-request-num')?.value?.trim()||null,
    already_performed: KB.alreadyDoneChecked,
    responsavel_id: KB.selectedResponsavel?.id||null,
    responsavel_name: KB.selectedResponsavel?.name||null,
    responsavel_email: KB.selectedResponsavel?.email||null,
    status: sendStatus,
    scheduled_date: document.getElementById('f-scheduled-date')?.value||null,
    start_date: document.getElementById('f-start-date')?.value||null,
    subtasks: KB.subtasks.filter(Boolean),
    resources: KB.resources.filter(Boolean),
  };

  try {
    const res=await apiFetch('/work-orders',{ method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body) });
    if (!res.ok) { const err=await res.json().catch(()=>({})); throw new Error(err.error||`HTTP ${res.status}`); }
    const created=await res.json();
    closeWizard();
    showToast(`OS #${created.os_number||created.id} gerada!`,'success');
    await loadAllColumns({ silent:true });
  } catch(e) {
    console.error('[WZ] submit',e);
    showToast(`Erro: ${e.message}`,'error');
  } finally {
    btn.disabled=false;
    btn.innerHTML='<svg viewBox="0 0 20 20" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" style="width:15px;height:15px"><polyline points="4 10 8 14 16 6"/></svg> Gerar OS';
  }
}

// ─── SUBTASKS / RESOURCES ─────────────────────────────────────────────────────
function addSubtask() {
  const list=document.getElementById('subtasksList');
  list.querySelector('.wz-subtasks-empty')?.remove();
  const idx=KB.subtasks.length; KB.subtasks.push('');
  const item=document.createElement('div'); item.className='wz-subtask-item';
  item.innerHTML=`<i class="fa-regular fa-circle-check" style="color:var(--text-muted);font-size:14px;"></i><input type="text" placeholder="Descrição da subtarefa..." /><button class="wz-subtask-remove"><i class="fa-solid fa-xmark"></i></button>`;
  const input=item.querySelector('input');
  input.addEventListener('input',()=>{ KB.subtasks[idx]=input.value; });
  item.querySelector('.wz-subtask-remove').addEventListener('click',()=>{
    KB.subtasks[idx]=null; item.remove();
    if (!list.querySelector('.wz-subtask-item')) list.innerHTML='<div class="wz-subtasks-empty"><i class="fa-regular fa-circle-check"></i><span>Nenhuma subtarefa adicionada</span></div>';
  });
  list.appendChild(item); input.focus();
}

function addResource() {
  const list=document.getElementById('resourcesList');
  list.querySelector('.wz-subtasks-empty')?.remove();
  const idx=KB.resources.length; KB.resources.push('');
  const item=document.createElement('div'); item.className='wz-resource-item';
  item.innerHTML=`<i class="fa-solid fa-wrench" style="color:var(--text-muted);font-size:13px;"></i><input type="text" placeholder="Nome do recurso..." /><button class="wz-subtask-remove"><i class="fa-solid fa-xmark"></i></button>`;
  const input=item.querySelector('input');
  input.addEventListener('input',()=>{ KB.resources[idx]=input.value; });
  item.querySelector('.wz-subtask-remove').addEventListener('click',()=>{
    KB.resources[idx]=null; item.remove();
    if (!list.querySelector('.wz-resource-item')) list.innerHTML='<div class="wz-subtasks-empty"><i class="fa-regular fa-toolbox"></i><span>Nenhum recurso adicionado</span></div>';
  });
  list.appendChild(item); input.focus();
}

// ─── ASSET DRAWER ─────────────────────────────────────────────────────────────
function bindAssetDrawer() {
  document.getElementById('assetSearchBtn')?.addEventListener('click', openAssetDrawer);
  document.getElementById('assetDrawerBack')?.addEventListener('click', closeAssetDrawer);
  document.getElementById('assetDrawerOverlay')?.addEventListener('click', closeAssetDrawer);
  document.getElementById('assetClearBtn')?.addEventListener('click', clearAsset);
  document.getElementById('assetFilterToggle')?.addEventListener('click', () => document.getElementById('assetFilterPanel')?.classList.remove('hidden'));
  document.getElementById('assetFilterBack')?.addEventListener('click', () => document.getElementById('assetFilterPanel')?.classList.add('hidden'));
  document.getElementById('afpApplyBtn')?.addEventListener('click', () => {
    KB.assetFilters={ location:document.getElementById('flt-location')?.value||'', asset_type:document.getElementById('flt-asset-type')?.value||'', code:document.getElementById('flt-code')?.value||'', criticality:document.getElementById('flt-criticality')?.value||'' };
    document.getElementById('assetFilterPanel')?.classList.add('hidden');
    searchAssets();
  });
  document.getElementById('afpClearBtn')?.addEventListener('click', () => {
    KB.assetFilters={};
    ['flt-location','flt-asset-type','flt-desc','flt-code','flt-unit','flt-barcode','flt-criticality','flt-type'].forEach(id=>{ const el=document.getElementById(id); if(el) el.value=''; });
    document.getElementById('assetFilterPanel')?.classList.add('hidden');
    searchAssets();
  });
  document.getElementById('assetSearchInput')?.addEventListener('input', e => {
    KB.assetSearch=e.target.value;
    clearTimeout(KB._assetTimer);
    KB._assetTimer=setTimeout(searchAssets,350);
  });
}

function openAssetDrawer() {
  document.getElementById('assetDrawerOverlay')?.classList.remove('hidden');
  document.getElementById('assetDrawer')?.classList.remove('hidden');
  document.getElementById('assetFilterPanel')?.classList.add('hidden');
  KB.assetSearch=''; KB.assetFilters={};
  const inp=document.getElementById('assetSearchInput'); if(inp) inp.value='';
  searchAssets();
}
function closeAssetDrawer() {
  document.getElementById('assetDrawerOverlay')?.classList.add('hidden');
  document.getElementById('assetDrawer')?.classList.add('hidden');
}
function clearAsset() {
  KB.selectedAsset=null;
  document.getElementById('assetSelected')?.classList.add('hidden');
  document.getElementById('assetSearchBtn')?.classList.remove('hidden');
}

async function searchAssets() {
  const loading=document.getElementById('assetLoading');
  const empty=document.getElementById('assetEmpty');
  const list=document.getElementById('assetResultsList');
  const footer=document.getElementById('assetResultsFooter');
  loading?.classList.remove('hidden'); empty?.classList.add('hidden');
  if(list) list.innerHTML=''; if(footer) footer.textContent='';

  const params=new URLSearchParams({ page:1, page_size:50 });
  if(KB.assetSearch) params.set('q',KB.assetSearch);
  Object.entries(KB.assetFilters).forEach(([k,v])=>{ if(v) params.set(k,v); });

  try {
    const res=await apiFetch(`/assets?${params}`);
    if(!res.ok) throw new Error(`HTTP ${res.status}`);
    const data=await res.json();
    const items=data.items||data.assets||[];
    loading?.classList.add('hidden');
    if(!items.length){ empty?.classList.remove('hidden'); return; }
    items.forEach(asset=>{
      const el=document.createElement('div'); el.className='asset-item';
      el.innerHTML=`
        <div class="asset-item-icon"><i class="fa-solid fa-microchip"></i></div>
        <div class="asset-item-body">
          <div class="asset-item-name">${esc(asset.name||asset.description)}</div>
          <div class="asset-item-code">${esc(asset.code||asset.asset_code||'')}</div>
          <div class="asset-item-meta">
            <div class="asset-meta-row"><span>Tipo:</span><span>${esc(asset.asset_type||'Instalações')}</span></div>
            <div class="asset-meta-row"><span>Local:</span><span>${esc(asset.location||asset.plant_name||'—')}</span></div>
            ${asset.criticality?`<div class="asset-meta-row"><span>Criticidade:</span><span>${esc(asset.criticality)}</span></div>`:''}
          </div>
        </div>`;
      el.addEventListener('click',()=>selectAsset(asset));
      list?.appendChild(el);
    });
    if(footer) footer.textContent=`Mostrando ${items.length} de ${data.total||items.length}`;
  } catch(e) {
    loading?.classList.add('hidden');
    if(list) list.innerHTML=`<div class="asset-empty"><i class="fa-solid fa-triangle-exclamation"></i><span>Erro ao buscar ativos</span></div>`;
  }
}

function selectAsset(asset) {
  KB.selectedAsset={ id:asset.id||asset.device_id, name:asset.name||asset.description, code:asset.code||asset.asset_code, location:asset.location||asset.plant_name, plant_id:asset.plant_id };
  document.getElementById('assetSelected')?.classList.remove('hidden');
  document.getElementById('assetSearchBtn')?.classList.add('hidden');
  document.getElementById('assetSelectedName').textContent=`${KB.selectedAsset.name}${KB.selectedAsset.code?` { ${KB.selectedAsset.code} }`:''}`;
  document.getElementById('err-asset')?.classList.add('hidden');
  document.getElementById('assetSearchBtn')?.classList.remove('has-error');
  closeAssetDrawer();
}

// ─── RESPONSÁVEL DRAWER ───────────────────────────────────────────────────────
function bindRespDrawer() {
  document.getElementById('responsavelBtn')?.addEventListener('click', openRespDrawer);
  document.getElementById('respDrawerBack')?.addEventListener('click', closeRespDrawer);
  document.getElementById('respDrawerOverlay')?.addEventListener('click', closeRespDrawer);

  document.getElementById('respSearchInput')?.addEventListener('input', e => {
    clearTimeout(KB._respTimer);
    KB._respTimer=setTimeout(()=>filterRespTable(e.target.value), 300);
  });
  document.getElementById('respSearchClear')?.addEventListener('click', ()=>{
    const inp=document.getElementById('respSearchInput'); if(inp) inp.value='';
    filterRespTable('');
  });
  document.getElementById('respDatePicker')?.addEventListener('change', loadRespUsers);
}

function openRespDrawer() {
  document.getElementById('respDrawerOverlay')?.classList.remove('hidden');
  document.getElementById('respDrawer')?.classList.remove('hidden');
  const today=new Date().toISOString().split('T')[0];
  const picker=document.getElementById('respDatePicker');
  if(picker&&!picker.value) picker.value=today;
  if(!KB.respUsers.length) loadRespUsers();
  else renderRespTable(KB.respUsers);
}
function closeRespDrawer() {
  document.getElementById('respDrawerOverlay')?.classList.add('hidden');
  document.getElementById('respDrawer')?.classList.add('hidden');
}

async function loadRespUsers() {
  const tbody=document.getElementById('respTableBody');
  if(tbody) tbody.innerHTML=`<tr><td colspan="9" style="text-align:center;padding:24px;color:var(--text-muted);"><span class="kb-spinner" style="display:inline-block;vertical-align:middle;margin-right:8px;"></span>Carregando...</td></tr>`;
  try {
    const date=document.getElementById('respDatePicker')?.value||new Date().toISOString().split('T')[0];
    const res=await apiFetch(`/users?date=${date}`);
    if(!res.ok) throw new Error(`HTTP ${res.status}`);
    const data=await res.json();
    KB.respUsers=data.users||data||[];
    renderRespTable(KB.respUsers);
  } catch(e) {
    console.warn('[RESP] load error',e);
    KB.respUsers=[];
    renderRespTable([]);
  }
}

const WEEK_DAYS=['Segunda-feira','Terça-feira','Quarta-feira','Quinta-feira','Sexta-feira','Sábado'];

function renderRespTable(users) {
  const tbody=document.getElementById('respTableBody');
  if(!tbody) return;
  if(!users.length){
    tbody.innerHTML=`<tr><td colspan="9" style="text-align:center;padding:24px;color:var(--text-muted);">Nenhum responsável encontrado</td></tr>`;
    return;
  }
  tbody.innerHTML=users.map(u=>{
    const hours=u.week_hours||{};
    const dayCells=WEEK_DAYS.map(d=>{
      const h=hours[d]||null;
      const cls=h?'resp-hours-cell has-hours':'resp-hours-cell';
      return `<td><span class="${cls}">${h||'SEM HORAS'}</span></td>`;
    }).join('');
    return `<tr data-user-id="${u.id||u.user_id}" data-user-name="${esc(u.name||u.username)}" data-user-email="${esc(u.email||'')}">
      <td style="font-family:'JetBrains Mono',monospace;font-size:11px;color:var(--text-muted);">${esc(u.code||u.id||'')}</td>
      <td>${esc(u.name||u.username)}</td>
      <td style="font-size:11px;color:var(--text-muted);">${esc(u.email||'')}</td>
      ${dayCells}
    </tr>`;
  }).join('');

  tbody.querySelectorAll('tr[data-user-id]').forEach(row=>{
    row.addEventListener('click',()=>{
      tbody.querySelectorAll('tr').forEach(r=>r.classList.remove('selected'));
      row.classList.add('selected');
      selectResponsavel({ id:row.dataset.userId, name:row.dataset.userName, email:row.dataset.userEmail });
    });
  });
}

function filterRespTable(q) {
  const filtered=!q?KB.respUsers:KB.respUsers.filter(u=>{
    const search=(u.name||u.username||'')+(u.email||'')+(u.code||'');
    return search.toLowerCase().includes(q.toLowerCase());
  });
  renderRespTable(filtered);
}

function selectResponsavel(user) {
  KB.selectedResponsavel=user;
  const label=document.getElementById('responsavelBtnLabel');
  if(label) label.textContent=user.name;
  document.getElementById('err-responsavel')?.classList.add('hidden');
  closeRespDrawer();
}

// ─── UTILS ────────────────────────────────────────────────────────────────────
function esc(str){ if(str==null) return ''; return String(str).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }
function fmtDate(iso){ if(!iso) return '—'; try{ return new Date(iso).toLocaleDateString('pt-BR',{day:'2-digit',month:'2-digit',year:'2-digit'}); }catch{ return '—'; } }
function fmtDatetime(iso){ if(!iso) return '—'; try{ return new Date(iso).toLocaleString('pt-BR',{day:'2-digit',month:'2-digit',year:'2-digit',hour:'2-digit',minute:'2-digit'}); }catch{ return '—'; } }

let _toastTimer=null;
function showToast(msg,type='success'){
  const el=document.getElementById('osToast');
  el.textContent=msg; el.className=`os-toast os-toast--${type}`;
  clearTimeout(_toastTimer);
  _toastTimer=setTimeout(()=>el.classList.add('hidden'),3500);
}
