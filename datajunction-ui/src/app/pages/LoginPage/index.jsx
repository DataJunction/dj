import { useState } from 'react';
import '../../../styles/login.css';
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
