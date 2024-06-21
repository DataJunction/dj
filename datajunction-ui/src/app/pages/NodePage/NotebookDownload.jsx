import DJClientContext from '../../providers/djclient';
import { useContext } from 'react';

export default function NotebookDownload({ node }) {
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
    <>
      <div
        className="badge download_notebook"
        style={{cursor: 'pointer', backgroundColor: '#ffefd0'}}
        tabIndex="0"
        height="45px"
        onClick={downloadFile}
      >
        Export as Notebook
      </div>
    </>
  );
}
