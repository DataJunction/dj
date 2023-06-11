import { DataJunctionAPI } from '../services/DJService';
import * as React from 'react';

const DJClientContext = React.createContext({ DataJunctionAPI });
export default DJClientContext;
