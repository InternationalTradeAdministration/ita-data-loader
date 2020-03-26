const axios = require('axios')

export default class Repository {
  async _save (containerName, pii, file) {
    let fileSaveResponse = await axios({
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
    let businessUnitResponse = await axios.get('/api/business-units')
    return businessUnitResponse.data
  }

  async _getDataloaderConfig (containerName) {
    let dataSetConfigsResponse = await axios.get('/api/configuration', {
      params: {
        containerName
      }
    })

    return dataSetConfigsResponse.data
  }

  async _saveDataloaderConfig (dataloaderConfig, containerName) {
    let configSaveResponse = await axios({
      url: '/api/configuration',
      method: 'PUT',
      headers: {
        'Content-Type': 'application/json'
      },
      params: {
        containerName
      },
      data: dataloaderConfig
    })
    return configSaveResponse
  }

  async _getStorageMetadata (containerName) {
    let storageMetadataResponse = await axios.get('/api/container-metadata', {
      params: {
        containerName
      }
    })

    return storageMetadataResponse.data
  }

  async _startIngestProcess (containerName) {
    let ingestProcessResponse = await axios.get('/api/ingest', {
      params: {
        containerName
      }
    })

    return ingestProcessResponse
  }

  async _getIngestStatus (containerName) {
    let ingestStatusResponse = await axios.get('/api/automated-ingest/status', {
      params: {
        containerName
      }
    })
    return ingestStatusResponse.data
  }

  async _getPipelineStatus (pipelineName) {
    let pipelineStatusResponse = await axios.get('/api/data-factory/pipeline-status', {
      params: {
        pipelineName
      }
    })
    return pipelineStatusResponse.data ? pipelineStatusResponse.data : null
  }

  async _getManualIngestStatus (containerName) {
    let uploadStatus = await axios.get('/api/manual-ingest/status', {
      params: {
        containerName
      }
    })
    return uploadStatus.data
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
}
