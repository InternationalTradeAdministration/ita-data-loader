const axios = require('axios')

export default class Repository {
  async _save(containerName, pii, file) {
    const fileSaveResponse = await axios.put('/api/ingest/file',
      file,
      {
        params: {
          containerName,
          pii
        }
      })
    return fileSaveResponse.data
  }

  async _getBusinessUnits() {
    const businessUnitResponse = await axios.get('/api/business-units')
    return businessUnitResponse.data
  }

  async _getAutomatedIngestConfig(containerName) {
    const dataSetConfigsResponse = await axios.get('/api/configuration/automated-ingest', {
      params: {
        containerName
      }
    })

    return dataSetConfigsResponse.data
  }

  async _getDataloaderAdminConfig() {
    const dataSetConfigsResponse = await axios.get('/api/configuration/dataloader-admin')
    return dataSetConfigsResponse.data
  }

  async _saveDataloaderAdminConfig(businessUnits) {
    const configSaveResponse = await axios.put('/api/configuration/dataloader-admin', {
      businessUnits
    })
    return configSaveResponse
  }

  async _saveAutomatedIngestConfig(dataSetConfigs, containerName) {
    const configSaveResponse = await axios.put('/api/configuration/automated-ingest',
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

  async _getStorageMetadata(containerName) {
    const storageMetadataResponse = await axios.get('/api/storage/container/blobs', {
      params: {
        containerName
      }
    })

    return storageMetadataResponse.data
  }

  async _startAutomatedIngestProcess(containerName, dataSetConfigs) {
    const ingestProcessResponse = await axios.post('/api/ingest/process', {
      dataSetConfigs
    }, {
      params: {
        containerName
      }
    })

    return ingestProcessResponse.data
  }

  async _getAutomatedIngestStatus(containerName) {
    const ingestStatusResponse = await axios.get('/api/ingest/automated/status', {
      params: {
        containerName
      }
    })
    return ingestStatusResponse.data
  }

  _clearAutomatedIngestStatus(containerName) {
    axios.get('/api/ingest/automated/log/clear', {
      params: {
        containerName
      }
    })
  }

  _stopAutomatedIngestProcess(containerName) {
    axios.get('/api/ingest/automated/stop', {
      params: {
        containerName
      }
    })
  }

  async _getManualIngestStatus(containerName) {
    const ingestStatusResponse = await axios.get('/api/ingest/manual/status', {
      params: {
        containerName
      }
    })
    return ingestStatusResponse.data
  }

  _clearManualIngestStatus(containerName) {
    axios.get('/api/ingest/manual/log/clear', {
      params: {
        containerName
      }
    })
  }

  async _runPipeline(pipelineName) {
    const pipelineStatusResponse = await axios.get('/api/data-factory/run-pipeline', {
      params: {
        pipelineName
      }
    })
    return pipelineStatusResponse.data ? pipelineStatusResponse.data : null
  }

  async _getPipelineStatus(pipelineName) {
    const pipelineStatusResponse = await axios.get('/api/data-factory/pipeline-status', {
      params: {
        pipelineName
      }
    })
    return pipelineStatusResponse.data ? pipelineStatusResponse.data : null
  }

  async _deleteBlob(containerName, fileName, snapshot) {
    await axios.delete('/api/storage/file', {
      params: {
        containerName,
        fileName,
        snapshot
      }
    })
  }

  async _version() {
    const version = await axios.get('/api/version')
    return version.data
  }

  async _downloadBlob(containerName, blobName, snapshot) {
    axios.post('/api/storage/download', {
      containerName, blobName, snapshot
    }, {
      responseType: 'blob'
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

  async _isContainerPublic(containerName) {
    return axios.get('/api/storage/container/public', {
      params: {
        containerName
      }
    }).then(response => response.data)

  }
}
