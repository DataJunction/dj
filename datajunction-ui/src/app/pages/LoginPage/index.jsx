import { useState } from 'react';
import { Formik, Form, Field, ErrorMessage } from 'formik';
import '../../../styles/login.css';
import logo from '../Root/assets/dj-logo.png';
import GitHubLoginButton from './assets/sign-in-with-github.png';
import GoogleLoginButton from './assets/sign-in-with-google.png';
import * as Yup from 'yup';
import SignupForm from './SignupForm';
import LoginForm from './LoginForm';

export function LoginPage() {
  const [showSignup, setShowSignup] = useState(false);
  return (
    <div className="container login">
      {showSignup ? (
        <SignupForm setShowSignup={setShowSignup} />
      ) : (
        <LoginForm setShowSignup={setShowSignup} />
      )}
    </div>
  );
}
