const axios = require('axios')

export default class Repository {
  async _save (containerName, pii, file) {
    const fileSaveResponse = await axios({
      url: '/api/file',
      method: 'PUT',
      params: {
        containerName,
        pii
      },
      data: file
    })
    return fileSaveResponse.data
  }

  async _getBusinessUnits () {
    const businessUnitResponse = await axios.get('/api/business-units')
    return businessUnitResponse.data
  }

  async _getAutomatedIngestConfig (containerName) {
    const dataSetConfigsResponse = await axios.get('/api/automated-ingest/configuration', {
      params: {
        containerName
      }
    })

    return dataSetConfigsResponse.data
  }

  async _getDataloaderAdminConfig () {
    const dataSetConfigsResponse = await axios.get('/api/dataloader-admin/configuration')
    return dataSetConfigsResponse.data
  }

  async _saveDataloaderAdminConfig (businessUnits) {
    const configSaveResponse = await axios({
      url: '/api/dataloader-admin/configuration',
      method: 'PUT',
      headers: {
        'Content-Type': 'application/json'
      },
      data: {
        businessUnits
      }
    })
    return configSaveResponse
  }

  async _saveAutomatedIngestConfig (dataSetConfigs, containerName) {
    const configSaveResponse = await axios.put('/api/automated-ingest/configuration',
      {
        dataSetConfigs
      },
      {
        params: {
          containerName
        }
      }
    )
    return configSaveResponse
  }

  async _getStorageMetadata (containerName) {
    const storageMetadataResponse = await axios.get('/api/container-metadata', {
      params: {
        containerName
      }
    })

    return storageMetadataResponse.data
  }

  async _startAutomatedIngestProcess (containerName, dataSetConfigs) {
    const ingestProcessResponse = await axios.post('/api/ingest', {
      dataSetConfigs
    }, {
      params: {
        containerName
      }
    })

    return ingestProcessResponse.data
  }

  async _getAutomatedIngestStatus (containerName) {
    const ingestStatusResponse = await axios.get('/api/automated-ingest/status', {
      params: {
        containerName
      }
    })
    return ingestStatusResponse.data
  }

  _clearAutomatedIngestStatus (containerName) {
    axios.get('/api/automated-ingest/log/clear', {
      params: {
        containerName
      }
    })
  }

  _stopAutomatedIngestProcess (containerName) {
    axios.get('/api/automated-ingest/stop', {
      params: {
        containerName
      }
    })
  }

  async _getManualIngestStatus (containerName) {
    const ingestStatusResponse = await axios.get('/api/manual-ingest/status', {
      params: {
        containerName
      }
    })
    return ingestStatusResponse.data
  }

  _clearManualIngestStatus (containerName) {
    axios.get('/api/manual-ingest/log/clear', {
      params: {
        containerName
      }
    })
  }

  async _runPipeline (pipelineName) {
    const pipelineStatusResponse = await axios.get('/api/data-factory/run-pipeline', {
      params: {
        pipelineName
      }
    })
    return pipelineStatusResponse.data ? pipelineStatusResponse.data : null
  }

  async _getPipelineStatus (pipelineName) {
    const pipelineStatusResponse = await axios.get('/api/data-factory/pipeline-status', {
      params: {
        pipelineName
      }
    })
    return pipelineStatusResponse.data ? pipelineStatusResponse.data : null
  }

  async _deleteBlob (containerName, fileName, snapshot) {
    await axios({
      url: '/api/file',
      method: 'DELETE',
      params: {
        containerName,
        fileName,
        snapshot
      }
    })
  }

  async _version () {
    const version = await axios.get('/api/version')
    return version.data
  }

  async _downloadBlob (containerName, blobName, snapshot) {
    axios({
      url: '/api/download-blob',
      method: 'POST',
      responseType: 'blob',
      data: {
        containerName, blobName, snapshot
      }
    }).then((response) => {
      const linkhref = window.URL.createObjectURL(new window.Blob([response.data]))
      const link = document.createElement('a')
      link.href = linkhref
      const blobDownloadName = snapshot === null ? blobName : blobName + '_' + snapshot
      link.setAttribute('download', blobDownloadName)
      document.body.appendChild(link)
      link.click()
    })
  }
}
