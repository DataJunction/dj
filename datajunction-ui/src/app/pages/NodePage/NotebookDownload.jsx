import DJClientContext from '../../providers/djclient';
import JupyterExportIcon from '../../icons/JupyterExportIcon';
import { useContext } from 'react';

export default function NotebookDownload({ node, buttonStyle }) {
  const djClient = useContext(DJClientContext).DataJunctionAPI;

  const downloadFile = async () => {
    try {
      const response = await djClient.notebookExportCube(node.name);
      const notebook = await response.blob();
      const url = window.URL.createObjectURL(new Blob([notebook]));

      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', 'notebook.ipynb');
      document.body.appendChild(link);
      link.click();
      link.parentNode.removeChild(link);
    } catch (error) {
      console.error('Error downloading file: ', error);
    }
  };

  return (
    <button
      onClick={downloadFile}
      style={buttonStyle}
    >
      <JupyterExportIcon /> Notebook
    </button>
  );
}
