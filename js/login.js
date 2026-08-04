// ============================================================
// CONFIG
// ============================================================

const API_BASE = "https://jgeg9i0js1.execute-api.us-east-1.amazonaws.com";

// ============================================================
// ELEMENTOS
// ============================================================

const form = document.getElementById("loginForm");
const errorBox = document.getElementById("loginError");
const btn = form.querySelector("button");

// ============================================================
// AUTO REDIRECT (SE JÁ ESTIVER LOGADO)
// 🔥 COMPORTAMENTO PROFISSIONAL
//
// 👉 Se existir "user" no localStorage:
//     - o login NÃO aparece
//     - redireciona direto para o resumo
//
// 👉 É por isso que, ao abrir index.html,
//     às vezes você "pula" o login
// ============================================================

const existingUser = localStorage.getItem("user");

if (existingUser) {
  window.location.replace("resumo.html");
}

// ============================================================
// HELPERS
// ============================================================

function showError(msg) {
  errorBox.innerText = msg;
  errorBox.style.display = "block";
}

function hideError() {
  errorBox.style.display = "none";
}

function setLoading(isLoading) {
  btn.disabled = isLoading;
  btn.innerText = isLoading ? "Entrando..." : "Entrar";
}

// ============================================================
// LOGIN (API)
// ============================================================

async function login(username, password) {
  const res = await fetch(`${API_BASE}/auth/login`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ username, password })
  });

  const data = await res.json();

  if (!res.ok || !data.ok) {
    throw new Error(data.error || "Erro ao autenticar");
  }

  return data; // { ok, token, user }
}

// ============================================================
// PRIMEIRO ACESSO — troca obrigatória da senha temporária
// ============================================================
// A tela aparece por cima do login, com a senha temporária já em mãos (a
// pessoa acabou de digitar), então não pedimos a senha atual de novo.
// Só salva o login e redireciona DEPOIS que a troca deu certo: enquanto a
// senha for a que o admin entregou, não existe sessão salva.

function forcarTrocaDeSenha(normalizedUser, senhaTemporaria) {
  return new Promise((resolve) => {
    const ov = document.createElement("div");
    ov.className = "fp-overlay";
    ov.innerHTML = `
      <div class="fp-card">
        <h2>Crie sua senha</h2>
        <p>Você entrou com uma senha temporária, que foi gerada por quem cadastrou
           seu acesso. Escolha agora uma senha só sua — ninguém mais vai conhecê-la.</p>
        <div class="fp-err" id="fpErr" style="display:none"></div>
        <label for="fpNew">Nova senha (mínimo 8 caracteres)</label>
        <input type="password" id="fpNew" autocomplete="new-password">
        <label for="fpNew2">Repita a nova senha</label>
        <input type="password" id="fpNew2" autocomplete="new-password">
        <button type="button" id="fpSave">Salvar e entrar</button>
        <a href="#" id="fpCancel">Cancelar e voltar ao login</a>
      </div>`;
    injetarEstiloPrimeiroAcesso();
    document.body.appendChild(ov);

    const errBox = ov.querySelector("#fpErr");
    const novo = ov.querySelector("#fpNew");
    const novo2 = ov.querySelector("#fpNew2");
    const btn = ov.querySelector("#fpSave");
    const erro = (m) => { errBox.textContent = m; errBox.style.display = "block"; };
    novo.focus();

    async function salvar() {
      errBox.style.display = "none";
      if (novo.value.length < 8) return erro("A senha precisa ter pelo menos 8 caracteres.");
      if (novo.value !== novo2.value) return erro("As duas senhas não são iguais.");
      if (novo.value === senhaTemporaria) {
        return erro("Escolha uma senha diferente da temporária que você recebeu.");
      }

      btn.disabled = true;
      btn.textContent = "Salvando...";
      try {
        const res = await fetch(`${API_BASE}/auth/change-password`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            ...(normalizedUser.token ? { "Authorization": `Bearer ${normalizedUser.token}` } : {})
          },
          body: JSON.stringify({ current_password: senhaTemporaria, new_password: novo.value })
        });
        const data = await res.json().catch(() => ({}));
        if (!res.ok || !data.ok) {
          throw new Error(data.error || "Não foi possível trocar a senha.");
        }
        normalizedUser.must_change_password = false;
        localStorage.setItem("user", JSON.stringify(normalizedUser));
        window.location.replace("resumo.html");
        resolve(true);
      } catch (e) {
        erro(e.message || "Não foi possível trocar a senha.");
        btn.disabled = false;
        btn.textContent = "Salvar e entrar";
      }
    }

    btn.addEventListener("click", salvar);
    [novo, novo2].forEach(i => i.addEventListener("keydown", (ev) => {
      if (ev.key === "Enter") { ev.preventDefault(); salvar(); }
    }));

    // Saída sem trocar: recarrega o login. Não salva sessão nenhuma — a senha
    // temporária continua valendo e a pessoa tenta de novo depois.
    ov.querySelector("#fpCancel").addEventListener("click", (ev) => {
      ev.preventDefault();
      window.location.reload();
    });
  });
}

function injetarEstiloPrimeiroAcesso() {
  if (document.getElementById("fpStyle")) return;
  const st = document.createElement("style");
  st.id = "fpStyle";
  st.textContent = `
    .fp-overlay{position:fixed;inset:0;z-index:9999;display:flex;align-items:center;
      justify-content:center;background:rgba(4,18,12,.88);backdrop-filter:blur(4px);padding:20px;}
    .fp-card{width:min(420px,100%);background:#0d2018;border:1px solid rgba(57,229,140,.3);
      border-radius:16px;padding:26px;color:#e6f5ec;box-shadow:0 20px 60px rgba(0,0,0,.5);
      font-family:inherit;}
    .fp-card h2{margin:0 0 10px;font-size:1.25rem;}
    .fp-card p{margin:0 0 18px;font-size:.85rem;line-height:1.5;color:#9fc9b3;}
    .fp-card label{display:block;font-size:.72rem;font-weight:600;color:#7fa892;
      margin:12px 0 5px;text-transform:uppercase;letter-spacing:.04em;}
    .fp-card input{width:100%;box-sizing:border-box;background:rgba(255,255,255,.06);
      border:1px solid rgba(57,229,140,.25);border-radius:9px;padding:11px 12px;
      color:inherit;font-size:.92rem;}
    .fp-card input:focus{outline:none;border-color:rgba(57,229,140,.7);}
    .fp-card button{width:100%;margin-top:20px;padding:12px;border:0;border-radius:9px;
      background:#39e58c;color:#04120c;font-weight:700;font-size:.92rem;cursor:pointer;}
    .fp-card button:disabled{opacity:.6;cursor:default;}
    .fp-card #fpCancel{display:block;text-align:center;margin-top:14px;font-size:.76rem;
      color:#7fa892;text-decoration:none;}
    .fp-card #fpCancel:hover{text-decoration:underline;}
    .fp-err{background:rgba(255,90,90,.12);border:1px solid rgba(255,90,90,.4);
      color:#ffb3b3;border-radius:9px;padding:9px 11px;font-size:.8rem;margin-bottom:6px;}
  `;
  document.head.appendChild(st);
}

// ============================================================
// SUBMIT DO FORM
// ============================================================

form.addEventListener("submit", async (e) => {
  e.preventDefault();
  hideError();

  const username = document.getElementById("username").value.trim();
  const password = document.getElementById("password").value;

  if (!username || !password) {
    showError("Informe usuário e senha");
    return;
  }

  try {
    setLoading(true);

    const data = await login(username, password);
    const user = data.user;

    // 🔐 NORMALIZAÇÃO DO USUÁRIO
    const normalizedUser = {
      id: user.id,
      username: user.username,
      customer_id: user.customer_id,
      is_superuser: user.is_superuser === true || user.is_superuser === 1,
      role_key: user.role_key || "viewer",
      permissions: user.permissions || {},
      // token de sessão assinado — prova de identidade nos endpoints
      // sensíveis (gestão de usuários); null enquanto a Lambda antiga estiver no ar
      token: data.token || null
    };

    // Primeiro acesso: a senha veio do admin, então ela morre aqui. Só entra
    // na plataforma depois de escolher uma que ninguém mais conhece.
    // Acontece ANTES de salvar o login: sessão só existe com senha própria.
    if (user.must_change_password) {
      await forcarTrocaDeSenha(normalizedUser, password);
      return;
    }

    // 🔥 AQUI É ONDE O LOGIN "FICA SALVO"
    localStorage.setItem("user", JSON.stringify(normalizedUser));

    // 🔁 REDIRECIONA PARA O RESUMO
    window.location.replace("resumo.html");

  } catch (err) {
    console.error("LOGIN ERROR:", err);
    showError("Usuário ou senha inválidos");
  } finally {
    setLoading(false);
  }
});

// ============================================================
// ENTER FUNCIONA
// ============================================================

document.querySelectorAll("input").forEach(input => {
  input.addEventListener("keydown", e => {
    if (e.key === "Enter") {
      form.dispatchEvent(new Event("submit"));
    }
  });
});

// ============================================================
// LOGIN COM GOOGLE (GIS) — fase 2
// 👉 Preencha GOOGLE_CLIENT_ID com o Client ID (tipo Web) do Google Cloud.
//    Enquanto ficar no placeholder, o botão NÃO aparece e o login normal
//    continua funcionando igual. A conta precisa já existir no app_user
//    (mesmo e-mail do Google) — o admin cadastra antes.
// ============================================================
const GOOGLE_CLIENT_ID = "274456589325-u8hkuh3qubu0d25at79k9tgfvd8ap4b6.apps.googleusercontent.com";

async function loginWithGoogle(credential) {
  const res = await fetch(`${API_BASE}/auth/google`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ credential })
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok || !data.ok) {
    throw new Error(data.error || "Falha no login com Google");
  }
  return data; // { ok, token, user }
}

function handleGoogleCredential(response) {
  hideError();
  const credential = response && response.credential;
  if (!credential) { showError("Não recebi credencial do Google"); return; }

  loginWithGoogle(credential)
    .then((data) => {
      const user = data.user;
      const normalizedUser = {
        id: user.id,
        username: user.username,
        customer_id: user.customer_id,
        is_superuser: user.is_superuser === true || user.is_superuser === 1,
        role_key: user.role_key || "viewer",
        permissions: user.permissions || {},
        token: data.token || null
      };
      localStorage.setItem("user", JSON.stringify(normalizedUser));
      window.location.replace("resumo.html");
    })
    .catch((err) => {
      console.error("GOOGLE LOGIN ERROR:", err);
      showError(err.message || "Não foi possível entrar com o Google");
    });
}

function initGoogleLogin(attempt = 0) {
  const wrap  = document.getElementById("loginGoogleWrap");
  const btnEl = document.getElementById("googleBtn");
  if (!wrap || !btnEl) return;
  // client id ainda não configurado → mantém escondido (login normal segue)
  if (!GOOGLE_CLIENT_ID || GOOGLE_CLIENT_ID.startsWith("COLOQUE_")) return;
  // aguarda o script do GIS carregar (é async/defer)
  if (!(window.google && google.accounts && google.accounts.id)) {
    if (attempt < 20) setTimeout(() => initGoogleLogin(attempt + 1), 250);
    return;
  }
  google.accounts.id.initialize({
    client_id: GOOGLE_CLIENT_ID,
    callback: handleGoogleCredential
  });
  google.accounts.id.renderButton(btnEl, {
    theme: "outline", size: "large", width: 280, text: "signin_with", locale: "pt-BR"
  });
  wrap.style.display = "block";
}

window.addEventListener("load", () => initGoogleLogin());