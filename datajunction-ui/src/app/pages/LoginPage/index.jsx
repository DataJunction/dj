import { useState } from 'react';
import { Formik, Form, Field, ErrorMessage } from 'formik';
import '../../../styles/login.css';
import logo from '../Root/assets/dj-logo.png';
import GitHubLoginButton from './assets/sign-in-with-github.png';
import GoogleLoginButton from './assets/sign-in-with-google.png';

export function LoginPage() {
  const [, setError] = useState('');
  const githubLoginURL = new URL(
    '/github/login/',
    process.env.REACT_APP_DJ_URL,
  );
  const googleLoginURL = new URL(
    '/google/login/',
    process.env.REACT_APP_DJ_URL,
  );

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
    <div className="container">
      <div className="login">
        <center>
          <Formik
            initialValues={{
              username: '',
              password: '',
              target: window.location.pathname,
            }}
            validate={values => {
              const errors = {};
              if (!values.username) {
                errors.username = 'Required';
              }
              if (!values.password) {
                errors.password = 'Required';
              }
              return errors;
            }}
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
                <center>
                  <div className="inputContainer">
                    <ErrorMessage name="username" component="span" />
                    <Field type="text" name="username" placeholder="Username" />
                  </div>
                </center>
                <center>
                  <div>
                    <ErrorMessage name="password" component="span" />
                    <Field
                      type="password"
                      name="password"
                      placeholder="Password"
                    />
                  </div>
                </center>
                <button type="submit" disabled={isSubmitting}>
                  Login
                </button>
                <center>{'Or'}</center>
                <center>
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
                </center>
                <center>
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
                </center>
              </Form>
            )}
          </Formik>
        </center>
      </div>
    </div>
  );
}
