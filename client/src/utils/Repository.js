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

  async _getDataloaderConfig (containerName) {
    const dataSetConfigsResponse = await axios.get('/api/configuration', {
      params: {
        containerName
      }
    })

    return dataSetConfigsResponse.data
  }

  async _saveDataloaderConfig (dataloaderConfig, containerName) {
    const configSaveResponse = await axios({
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
    const storageMetadataResponse = await axios.get('/api/container-metadata', {
      params: {
        containerName
      }
    })

    return storageMetadataResponse.data
  }

  async _startIngestProcess (containerName) {
    const ingestProcessResponse = await axios.get('/api/ingest', {
      params: {
        containerName
      }
    })

    return ingestProcessResponse.data
  }

  async _getIngestStatus (containerName) {
    const ingestStatusResponse = await axios.get('/api/automated-ingest/status', {
      params: {
        containerName
      }
    })
    return ingestStatusResponse.data
  }

  _clearIngestStatus (containerName) {
    axios.get('/api/automated-ingest/log/clear', {
      params: {
        containerName
      }
    })
  }

  _stopIngestProcess (containerName) {
    axios.get('/api/automated-ingest/stop', {
      params: {
        containerName
      }
    })
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
}
