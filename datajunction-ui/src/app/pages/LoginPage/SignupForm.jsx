import { useState } from 'react';
import { Formik, Form, Field, ErrorMessage } from 'formik';
import '../../../styles/login.css';
import logo from '../Root/assets/dj-logo.png';
import LoadingIcon from '../../icons/LoadingIcon';
import GitHubLoginButton from './assets/sign-in-with-github.png';
import GoogleLoginButton from './assets/sign-in-with-google.png';
import * as Yup from 'yup';

const githubLoginURL = new URL('/github/login/', process.env.REACT_APP_DJ_URL);
const googleLoginURL = new URL('/google/login/', process.env.REACT_APP_DJ_URL);

const SignupSchema = Yup.object().shape({
  email: Yup.string().email('Invalid email').required('Email is required'),
  signupUsername: Yup.string()
    .min(3, 'Must be at least 2 characters')
    .max(20, 'Must be less than 20 characters')
    .required('Username is required'),
  signupPassword: Yup.string().required('Password is required'),
});

export default function SignupForm({ setShowSignup }) {
  const [, setError] = useState('');

  // Add the path that the user was trying to access in order to properly redirect after auth
  githubLoginURL.searchParams.append('target', window.location.pathname);
  googleLoginURL.searchParams.append('target', window.location.pathname);

  const handleBasicSignup = async ({
    email,
    signupUsername,
    signupPassword,
  }) => {
    const data = new FormData();
    data.append('email', email);
    data.append('username', signupUsername);
    data.append('password', signupPassword);
    await fetch(`${process.env.REACT_APP_DJ_URL}/basic/user/`, {
      method: 'POST',
      body: data,
      credentials: 'include',
    }).catch(error => {
      setError(error ? JSON.stringify(error) : '');
    });
    const loginData = new FormData();
    loginData.append('username', signupUsername);
    loginData.append('password', signupPassword);
    await fetch(`${process.env.REACT_APP_DJ_URL}/basic/login/`, {
      method: 'POST',
      body: data,
      credentials: 'include',
    }).catch(error => {
      setError(error ? JSON.stringify(error) : '');
    });
    window.location.reload();
  };

  return (
    <Formik
      initialValues={{
        email: '',
        signupUsername: '',
        signupPassword: '',
        target: window.location.pathname,
      }}
      validationSchema={SignupSchema}
      onSubmit={(values, { setSubmitting }) => {
        setTimeout(() => {
          handleBasicSignup(values);
          setSubmitting(false);
        }, 400);
      }}
    >
      {({ isSubmitting }) => (
        <Form>
          <div className="logo-title">
            <img src={logo} alt="DJ Logo" width="75px" height="75px" />
            <h2>DataJunction</h2>
          </div>
          <div>
            <Field type="text" name="email" placeholder="Email" />
          </div>
          <div>
            <ErrorMessage
              className="form-error"
              name="email"
              component="span"
            />
          </div>
          <div>
            <Field type="text" name="signupUsername" placeholder="Username" />
          </div>
          <div>
            <ErrorMessage
              className="form-error"
              name="signupUsername"
              component="span"
            />
          </div>
          <div>
            <Field
              type="password"
              name="signupPassword"
              placeholder="Password"
            />
          </div>
          <div>
            <ErrorMessage
              className="form-error"
              name="signupPassword"
              component="span"
            />
          </div>
          <div>
            <p>
              Have an account already?{' '}
              <a onClick={() => setShowSignup(false)}>Login</a>
            </p>
          </div>
          <button type="submit" disabled={isSubmitting}>
            {isSubmitting ? <LoadingIcon /> : 'Sign Up'}
          </button>
          <div>
            <p>Or</p>
          </div>
          {process.env.REACT_ENABLE_GITHUB_OAUTH === 'true' ? (
            <div>
              <a href={githubLoginURL.href}>
                <img
                  src={GitHubLoginButton}
                  alt="Sign in with GitHub"
                  width="200px"
                />
              </a>
            </div>
          ) : (
            ''
          )}
          {process.env.REACT_ENABLE_GOOGLE_OAUTH === 'true' ? (
            <div>
              <a href={googleLoginURL.href}>
                <img
                  src={GoogleLoginButton}
                  alt="Sign in with Google"
                  width="200px"
                />
              </a>
            </div>
          ) : (
            ''
          )}
        </Form>
      )}
    </Formik>
  );
}
