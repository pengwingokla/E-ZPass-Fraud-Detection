// ==========================
// OPEN / CLOSE MODAL
// ==========================
function openLoginModal() {
    const modal = document.getElementById("register_box_front");
    if (modal) {
        modal.style.display = "flex";
    } else {
        console.error("Modal element #register_box_front not found.");
    }
}

function closeLoginModal() {
    const modal = document.getElementById("register_box_front");
    if (modal) {
        modal.style.display = "none";
    }
}

// ==========================
// LOGIN LOGIC
// ==========================
document.addEventListener("DOMContentLoaded", () => {

    const modal = document.getElementById("register_box_front");

    // Close when clicking outside
    if (modal) {
        modal.addEventListener("click", (e) => {
            if (e.target === modal) closeLoginModal();
        });
    }

    // Hardcoded test user
    const USERS = {
        "ezpass": "ezpass"
    };

    const form = document.getElementById("modal-login-form");
    const errorEl = document.getElementById("modal-error");
    const captchaValueEl = document.getElementById("modal-captcha-value");
    const refreshBtn = document.getElementById("modal-refresh-captcha");
    const captchaInput = document.getElementById("modal-captcha-input");

    let captcha = "";

    function generateCaptcha() {
        captcha = (Math.floor(Math.random() * 9000) + 1000).toString();
        if (captchaValueEl) captchaValueEl.textContent = captcha;
    }
    generateCaptcha();

    if (refreshBtn) {
        refreshBtn.addEventListener("click", () => {
            generateCaptcha();
            captchaInput.value = "";
            errorEl.style.display = "none";
        });
    }

    if (form) {
        form.addEventListener("submit", (e) => {
            e.preventDefault();
            errorEl.style.display = "none";

            const username = document.getElementById("modal-username").value.trim().toLowerCase();
            const password = document.getElementById("modal-password").value.trim();
            const userCaptcha = captchaInput.value.trim();

            if (userCaptcha !== captcha) {
                errorEl.textContent = "Incorrect security message.";
                errorEl.style.display = "block";
                generateCaptcha();
                return;
            }

            if (!USERS[username] || USERS[username] !== password) {
                errorEl.textContent = "Invalid username or password.";
                errorEl.style.display = "block";
                return;
            }

            // SUCCESS
            sessionStorage.setItem("mockAuth", "true");
            sessionStorage.setItem("authenticatedUser", username);

            window.location.href = "index.html";
        });
    }
});
