import { useState } from 'react';
import { Formik, Form, Field, ErrorMessage } from 'formik';
import '../../../styles/login.css';
import logo from '../Root/assets/dj-logo.png';
import GitHubLoginButton from './assets/sign-in-with-github.png';

export function LoginPage({ onLogin }) {
  const [, setError] = useState('');
  const githubLoginURL = new URL('/github/login/', process.env.REACT_APP_DJ_URL)
    .href;
  function handleBasicLogin({ username, password }) {
    const data = new FormData();
    data.append('username', username);
    data.append('password', password);
    fetch(`${process.env.REACT_APP_DJ_URL}/basic/login/`, {
      method: 'POST',
      body: data,
      credentials: 'include',
    })
      .then(response => response.json())
      .then(data => onLogin(data.token))
      .catch(error => {
        setError(error ? JSON.stringify(error) : '');
      });
  }

  return (
    <div class="container">
      <div class="login">
        <center>
          <Formik
            initialValues={{ username: '', password: '' }}
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
                <div className="inputContainer">
                  <ErrorMessage name="username" component="span" />
                  <Field type="text" name="username" placeholder="Username" />
                </div>
                <div>
                  <ErrorMessage name="password" component="span" />
                  <Field
                    type="password"
                    name="password"
                    placeholder="Password"
                  />
                </div>
                <button type="submit" disabled={isSubmitting}>
                  Login
                </button>
                {process.env.REACT_ENABLE_GITHUB_OAUTH === 'true' ? (
                  <div>
                    <a href={githubLoginURL}>
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
              </Form>
            )}
          </Formik>
        </center>
      </div>
    </div>
  );
}
