import { useContext } from 'react';
import { Outlet } from 'react-router-dom';
import logo from './assets/dj-logo.svg';
import { Helmet } from 'react-helmet-async';
import DJClientContext from '../../providers/djclient';

export function Root() {
  const djClient = useContext(DJClientContext).DataJunctionAPI;

  const handleLogout = async () => {
    await djClient.logout();
    window.location.reload();
  };
  return (
    <>
      <Helmet>
        <title>DataJunction</title>
        <meta
          name="description"
          content="DataJunction Metrics Platform Webapp"
        />
      </Helmet>
      <div className="container d-flex align-items-center justify-content-between">
        <div className="header">
          <div className="logo">
            <h2>
              <svg width="30" height="30" style={{marginRight: '10px', marginBottom: '2px'}} viewBox="0 0 83 83" fill="none" xmlns="http://www.w3.org/2000/svg" xmlnsXlink="http://www.w3.org/1999/xlink">
              <rect width="83" height="83" fill="url(#pattern0)"/>
              <rect width="83" height="83" stroke="none"/>
              <defs>
              <pattern id="pattern0" patternContentUnits="objectBoundingBox" width="1" height="1">
              <use xlinkHref="#image0_1_2" transform="scale(0.00364964)"/>
              </pattern>
              <image id="image0_1_2" width="274" height="274" xlinkHref="data:image/png;base64,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"/>
              </defs>
              </svg>

              Data<b>Junction</b>
            </h2>
          </div>
          <div className="menu">
            <div className="menu-item here menu-here-bg menu-lg-down-accordion me-0 me-lg-2 fw-semibold">
              <span className="menu-link">
                <span className="menu-title">
                  <a href="/">Explore</a>
                </span>
              </span>
              <span className="menu-link">
                <span className="menu-title">
                  <a href="/sql">SQL</a>
                </span>
              </span>
              <span className="menu-link">
                <span className="menu-title">
                  <a
                    href="https://www.datajunction.io"
                    target="_blank"
                    rel="noreferrer"
                  >
                    Docs
                  </a>
                </span>
              </span>
            </div>
          </div>
        </div>
        {process.env.REACT_DISABLE_AUTH === 'true' ? (
          ''
        ) : (
          <span className="menu-link">
            <span className="menu-title">
              <button onClick={handleLogout}>Logout</button>
            </span>
          </span>
        )}
      </div>
      <Outlet />
    </>
  );
}
