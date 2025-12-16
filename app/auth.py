"""
Authentication Module for Streamlit App

Provides password-based authentication using Streamlit Secrets.
No passwords are stored in code - all secrets come from st.secrets.
"""

import hmac
import streamlit as st


def check_password(input_password: str, secret_password: str) -> bool:
    """
    Securely compare passwords using constant-time comparison.
    
    Args:
        input_password: Password entered by user
        secret_password: Expected password from secrets
        
    Returns:
        True if passwords match
    """
    return hmac.compare_digest(input_password, secret_password)


def logout():
    """
    Log out the current user by clearing authentication state.
    """
    if "authenticated" in st.session_state:
        del st.session_state["authenticated"]
    st.rerun()


def require_password():
    """
    Require password authentication before allowing access to the app.
    
    Reads configuration from st.secrets["auth"]:
    - required: bool - If False, skip authentication
    - password: str - The password to check against
    
    This function MUST be called at the top of each page.
    It calls st.stop() if not authenticated, preventing further execution.
    """
    # Check if auth is configured and required
    auth_config = st.secrets.get("auth", {})
    auth_required = auth_config.get("required", False)
    
    # If auth not required, allow access
    if not auth_required:
        return
    
    # Check if already authenticated
    if st.session_state.get("authenticated", False):
        return
    
    # Get the password from secrets
    secret_password = auth_config.get("password", "")
    
    if not secret_password:
        st.error("⚠️ Configuration erreur : mot de passe non configuré dans les secrets.")
        st.stop()
        return
    
    # Show login form
    st.markdown("## 🔐 Accès protégé")
    st.markdown("Cette application nécessite une authentification.")
    
    with st.form("login_form"):
        password_input = st.text_input(
            "Mot de passe",
            type="password",
            placeholder="Entrez le mot de passe"
        )
        submit = st.form_submit_button("Se connecter", use_container_width=True)
        
        if submit:
            if password_input and check_password(password_input, secret_password):
                st.session_state["authenticated"] = True
                st.rerun()
            else:
                st.error("❌ Mot de passe incorrect")
    
    # Stop execution - page content below require_password() won't run
    st.stop()


def show_logout_button():
    """
    Display a logout button in the sidebar.
    Only shows if user is authenticated.
    """
    if st.session_state.get("authenticated", False):
        with st.sidebar:
            st.markdown("---")
            if st.button("🚪 Se déconnecter", use_container_width=True):
                logout()
