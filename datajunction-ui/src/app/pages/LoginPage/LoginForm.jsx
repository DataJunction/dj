import { useState } from 'react';
import { Formik, Form, Field, ErrorMessage } from 'formik';
import '../../../styles/login.css';
import LoadingIcon from '../../icons/LoadingIcon';
import logo from '../Root/assets/dj-logo.png';
import GitHubLoginButton from './assets/sign-in-with-github.png';
import GoogleLoginButton from './assets/sign-in-with-google.png';
import * as Yup from 'yup';

const githubLoginURL = new URL('/github/login/', process.env.REACT_APP_DJ_URL);
const googleLoginURL = new URL('/google/login/', process.env.REACT_APP_DJ_URL);

const LoginSchema = Yup.object().shape({
  username: Yup.string()
    .min(2, 'Too Short')
    .max(20, 'Too Long')
    .required('Username is required'),
  password: Yup.string().required('Password is required'),
});

export default function LoginForm({setShowSignup}) {
  const [, setError] = useState('');

  // Add the path that the user was trying to access in order to properly redirect after auth
  githubLoginURL.searchParams.append('target', window.location.pathname);
  googleLoginURL.searchParams.append('target', window.location.pathname);

  const handleBasicLogin = async ({ username, password }) => {
    const data = new FormData();
    data.append('username', username);
    data.append('password', password);
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
        username: '',
        password: '',
        target: window.location.pathname,
      }}
      validationSchema={LoginSchema}
      onSubmit={(values, { setSubmitting }) => {
        setTimeout(() => {
          handleBasicLogin(values);
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
            <Field type="text" name="username" placeholder="Username" />
          </div>
          <div>
            <ErrorMessage name="username" component="span" />
          </div>
          <div>
            <Field type="password" name="password" placeholder="Password" />
          </div>
          <div>
            <ErrorMessage name="password" component="span" />
          </div>
          <div>
            <p>
              Don't have an account yet?{' '}
              <a onClick={() => setShowSignup(true)}>Sign Up</a>
            </p>
          </div>
          <button type="submit" disabled={isSubmitting}>
          {isSubmitting ? <LoadingIcon/> : "Login"}
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
