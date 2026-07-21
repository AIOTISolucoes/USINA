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