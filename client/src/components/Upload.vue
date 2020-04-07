<template>
  <div>
    <dataloader-header
      v-if="containerName"
      :businessUnits="businessUnits"
      :initialContainerName="containerName"
      :updateContainerFn="updateContainer"
    />
    <div class="content">
      <dataloader-menu :containerName="containerName" />
      <div class="sub-content">
        <div class="md-layout md-gutter">
          <div class="md-layout-item md-size-30">
            <md-field>
              <label>Select file</label>
              <md-file @md-change="onFileSelection($event)"></md-file>
            </md-field>
          </div>
          <div class="md-layout-item md-size-30">
            <md-autocomplete v-model="destinationFileName" :md-options="destinationFileNameOptions">
              <label>File name</label>
            </md-autocomplete>
          </div>
          <div class="md-layout-item md-size-10">
            <md-checkbox v-if="!uploading" v-model="containsPii">Contains PII</md-checkbox>
          </div>
          <div class="md-layout-item md-size-10">
            <md-button
              v-if="!uploading"
              class="md-primary md-raised top-btn"
              @click="uploadFile()"
            >Upload</md-button>
            <div v-if="uploading" class="spinner">
              <md-progress-spinner md-mode="indeterminate" :md-diameter="30"></md-progress-spinner>
            </div>
          </div>
        </div>
        <div v-if="loading" class="loading">
          <md-progress-bar md-mode="indeterminate"></md-progress-bar>
        </div>
        <div v-if="!loading" class="md-layout md-gutter">
          <div class="md-layout-item md-size-100">
            <span class="stat">
              <strong>TOTAL FILES:</strong>
              {{totalFiles}}
            </span>
            <span class="stat">
              <strong>UPLOADS:</strong>
              {{totalManualUploads}}
            </span>
            <span class="stat">
              <strong>PIPELINE:</strong>
              <span v-if="!['Succeeded', 'InProgress', 'n/a'].includes(this.pipelineStatus.status)">
                <a @click="displayPipelineMessage = true" href="#">Error</a>
              </span>
              <span v-else>{{this.pipelineStatus.status}}</span>
              <span>{{this.pipelineStatus.runEnd}}</span>
            </span>
            <md-button class="md-dense refresh-btn" @click="updateBusinessUnitContent()">Refresh</md-button>
            <md-switch
              class="display-switch"
              v-model="displaySnapshots"
              @change="updateBusinessUnitContent()"
            >Snapshots</md-switch>
            <md-switch class="display-switch" v-model="displayDeleteButton">
              Delete Button
              <md-tooltip
                md-direction="right"
              >To recover a deleted file, please contact technical support.</md-tooltip>
            </md-switch>
          </div>
        </div>
        <div v-if="!loading" class="md-layout storage-content">
          <md-table v-model="storageMetadata" md-sort="name" md-sort-order="asc">
            <md-table-row slot-scope="{ item }" slot="md-table-row">
              <md-table-cell md-label="Download">
                <md-button class="md-icon-button" @click="downloadBlob(item)">
                  <md-icon>get_app</md-icon>
                </md-button>
              </md-table-cell>
              <md-table-cell md-label="File Name" md-sort-by="fileName">
                <label
                  v-if="item.snapshot"
                >{{item.fileName + ' (' + item.snapshot + ')'}}</label>
                <label v-else>{{item.fileName}}</label>
              </md-table-cell>
              <md-table-cell md-label="Uploaded At" md-sort-by="uploadedAt">{{item.uploadedAt}}</md-table-cell>
              <md-table-cell
                md-label="Uploaded By"
                md-sort-by="metadata.uploaded_by"
              >{{item.metadata.uploaded_by}}</md-table-cell>
              <md-table-cell md-label="Size" md-sort-by="size" md-numeric>{{item.size}}</md-table-cell>
              <md-table-cell md-label="PII" md-sort-by="metadata.pii">
                <span v-if="item.metadata.pii === 'true'" class="dot filled"></span>
                <span v-else class="dot"></span>
              </md-table-cell>
              <md-table-cell md-label="Upload" md-sort-by="metadata.user_upload">
                <span v-if="item.metadata.user_upload === 'true'" class="dot filled"></span>
                <span v-else class="dot"></span>
              </md-table-cell>
              <md-table-cell md-label="Snapshot" md-sort-by="snapshot">
                <span v-if="item.snapshot" class="dot filled"></span>
                <span v-else class="dot"></span>
              </md-table-cell>
              <md-table-cell v-if="displayDeleteButton">
                <md-button
                  class="md-icon-button"
                  @click="deleteFile(item.fileName, item.snapshot)"
                >
                  <md-icon>delete</md-icon>
                </md-button>
              </md-table-cell>
            </md-table-row>
          </md-table>
        </div>
        <div class="user-feedback">
          <md-dialog-alert
            :md-active.sync="uploadSuccessful"
            md-title="Upload Successful!"
            md-content="Your file was uploaded successfully! "
            md-confirm-text="Close"
          />
          <md-dialog-alert
            :md-active.sync="errorOccured"
            md-title="Upload Error!"
            :md-content="errorMessage"
            md-confirm-text="Close"
          />
          <md-dialog-alert
            :md-active.sync="displayPipelineMessage"
            md-title="Pipeline Error!"
            :md-content="pipelineStatus.message"
            md-confirm-text="Close"
          />
        </div>
      </div>
    </div>
  </div>
</template>
<style>
.user-feedback {
  display: flex;
  justify-content: center;
}

.uploading {
  margin-top: 20px;
  display: flex;
  justify-content: center;
}

.storage-content {
  padding-top: 10px;
}

.dot {
  height: 10px;
  width: 10px;
  border-radius: 50%;
  display: inline-block;
  border: 1px solid black;
}
.dot.filled {
  background-color: #1b51ab;
  border: 1px solid #1b51ab;
}

.display-switch {
  margin: 0 22px 0 0;
}
</style>
<script>
import Menu from "./Menu";
import Header from "./Header";
import { readUploadedFileAsArrayBuffer } from "./FileHelper";

export default {
  name: "Upload",
  props: {
    repository: Object
  },
  components: {
    "dataloader-header": Header,
    "dataloader-menu": Menu
  },
  async created() {
    this.loading = true;
    this.businessUnits = await this.repository._getBusinessUnits();

    if (this.$route.params["containerName"]) {
      this.containerName = this.$route.params["containerName"];
    } else {
      this.containerName = this.businessUnits[0].containerName;
    }

    await this.updateBusinessUnitContent();

    this.loading = false;
  },
  data() {
    return {
      containerName: null,
      businessUnitName: null,
      businessUnits: [],
      storageMetadata: [],
      destinationFileNameOptions: [],
      originalFileName: null,
      destinationFileName: null,
      containsPii: false,
      errorOccured: false,
      errorMessage: null,
      uploadSuccessful: false,
      uploading: false,
      fileBlob: null,
      loading: true,
      totalFiles: 0,
      totalManualUploads: 0,
      pipelineStatus: {
        status: "n/a",
        message: "",
        runEnd: ""
      },
      displayPipelineMessage: false,
      displaySnapshots: false,
      displayDeleteButton: false
    };
  },
  methods: {
    async updateBusinessUnitContent() {
      this.loading = true;
      let storageMetadata = await this.repository._getStorageMetadata(
        this.containerName
      );

      this.storageMetadata = storageMetadata.filter(metadata => {
        if (this.displaySnapshots) return true;
        else return !metadata.snapshot;
      });

      this.totalFiles = this.storageMetadata.length;

      this.businessUnitName = this.businessUnits.find(
        b => b.containerName === this.containerName
      ).businessName;

      this.destinationFileNameOptions = this.storageMetadata
        .filter(file => file.metadata.user_upload === "true")
        .map(file => file.fileName);

      this.totalManualUploads = this.storageMetadata.filter(
        file => file.metadata.user_upload === "true"
      ).length;

      this.pipelineStatus = await this.repository._getPipelineStatus(
        this.containerName
      );

      if (this.pipelineStatus == null) {
        this.pipelineStatus = { status: "n/a", message: "", runEnd: "" };
      }
      this.loading = false;
    },
    onFileSelection(event) {
      this.fileBlob = event[0];
      this.originalFileName = event[0].name;
      this.uploadSuccessful = false;
    },
    async uploadFile() {
      this.uploading = true;
      this.uploadSuccessful = false;
      this.errorMessage = null;
      if (!this.fileBlob) {
        this.setErrorState("Please select a file to be uploaded.");
        return;
      }

      if (!this.destinationFileName) {
        this.setErrorState(
          "Please select a destination file name for the upload."
        );
        return;
      }

      const formData = new FormData();
      formData.append("file", this.fileBlob, this.destinationFileName);
      const message = await this.repository._save(
        this.containerName,
        this.containsPii,
        formData
      );
      this.uploadSuccessful = true;
      this.uploading = false;

      await this.updateBusinessUnitContent();
    },
    setErrorState(errorMessage) {
      this.errorOccured = true;
      this.errorMessage = errorMessage;
      this.uploadSuccessful = false;
      this.uploading = false;
    },
    async updateContainer(containerName) {
      this.containerName = containerName;
      await this.updateBusinessUnitContent();
      this.$forceUpdate();
    },
    deleteFile(fileName, snapshot) {
      this.repository._deleteBlob(this.containerName, fileName, snapshot);
      this.storageMetadata = this.storageMetadata.filter(meta =>
        !(meta.containerName === this.containerName &&
        meta.fileName === fileName &&
        meta.snapshot === snapshot)
      )
    },
    downloadBlob(blobItem) {
      this.repository._downloadBlob(
        blobItem.containerName,
        blobItem.fileName,
        blobItem.snapshot
      );
    }
  }
};
</script>
