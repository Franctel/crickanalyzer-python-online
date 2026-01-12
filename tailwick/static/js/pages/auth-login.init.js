/*
Template Name: Tailwick - Admin & Dashboard Template
Author: Themesdesign
Version: 1.1.0
Website: https://themesdesign.in/
Contact: Themesdesign@gmail.com
File: auth login init Js File
*/

document.addEventListener("DOMContentLoaded", function () {
    const form = document.getElementById('signInForm');
    if (!form) {
        // 🚪 Not on login page → skip this script
        return;
    }

    form.addEventListener('submit', function (event) {
        let blockSubmit = false; // ✅ only prevent submit if invalid

        // Get input elements
        const usernameInput = document.getElementById('username');
        const passwordInput = document.getElementById('password');
        const username = usernameInput ? usernameInput.value.trim() : "";
        const password = passwordInput ? passwordInput.value.trim() : "";

        // // Validation elements
        // const usernameError = document.getElementById('username-error');
        // const passwordError = document.getElementById('password-error');
        // const successAlert = document.getElementById('successAlert');
        // const rememberMeCheckbox = document.getElementById('checkboxDefault1');
        // const rememberError = document.getElementById('remember-error');

        // // Reset errors
        // if (usernameError) usernameError.classList.add('hidden');
        // if (passwordError) passwordError.classList.add('hidden');
        // if (successAlert) successAlert.classList.add('hidden');
        // if (rememberError) rememberError.classList.add('hidden');

        // // ✅ Username validation (only check non-empty)
        // if (!username) {
        //     if (usernameError) usernameError.classList.remove('hidden');
        //     blockSubmit = true;
        // }

        // // ✅ Password validation (only check non-empty)
        // if (!password) {
        //     if (passwordError) passwordError.classList.remove('hidden');
        //     blockSubmit = true;
        // }

        // // ✅ Remember Me validation
        // if (rememberMeCheckbox && !rememberMeCheckbox.checked) {
        //     if (rememberError) rememberError.classList.remove('hidden');
        //     blockSubmit = true;
        // }

        // ❌ Stop submit only if something failed
        if (blockSubmit) {
            event.preventDefault();
        } else {
            // Optional: show success message before backend processes login
            if (successAlert) successAlert.classList.remove('hidden');
            // Form submits normally → Flask handles actual login + redirect
        }
    });
});
