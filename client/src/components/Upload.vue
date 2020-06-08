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
          <div class="md-layout-item md-size-20">
            <md-field>
              <label>Select file</label>
              <md-file @md-change="onFileSelection($event)"></md-file>
            </md-field>
          </div>
          <div class="md-layout-item md-size-20">
            <md-autocomplete v-model="destinationFileName" :md-options="destinationFileNameOptions">
              <label>File name</label>
            </md-autocomplete>
          </div>
          <div class="md-layout-item md-size-60">
            <md-checkbox v-model="containsPii" v-if="!loading && !isContainerPublic">Contains PII</md-checkbox>
            <md-button
              class="md-raised md-dense top-btn"
              @click="updateBusinessUnitContent()"
            >Refresh</md-button>
            <md-button
              v-if="!uploading"
              class="md-primary md-raised md-dense top-btn"
              @click="uploadFile()"
            >Upload</md-button>
            <md-progress-spinner
              class="spinner"
              v-if="uploading"
              md-mode="indeterminate"
              :md-diameter="30"
            ></md-progress-spinner>
          </div>
        </div>
        <div v-if="loading" class="loading">loading...</div>
        <div v-else class="md-layout md-gutter">
          <div class="md-layout-item md-size-100">
            <span class="stat">
              <strong>CONTAINER TYPE:</strong>
              <span v-if="isContainerPublic">Public</span>
              <span v-else>Private</span>
            </span>
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
                <a
                  @click="alertPipelineErrorMessage()"
                  href="#"
                >Error-{{this.pipelineStatus.runEnd}}</a>
              </span>
              <span v-else>{{pipelineStatus.status}}-{{this.pipelineStatus.runEnd}}</span>
            </span>
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
                <label v-if="item.snapshot">{{item.fileName + ' (' + item.snapshot + ')'}}</label>
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
                <md-button class="md-icon-button" @click="deleteFile(item.fileName, item.snapshot)">
                  <md-icon>delete</md-icon>
                </md-button>
              </md-table-cell>
            </md-table-row>
          </md-table>
        </div>
      </div>
    </div>
    <dialog ref="upload-success-dialog">
      <h2>Upload Successful</h2>
      <p>Your file was uploaded successfully! To view the progress of any translations, see the log.</p>
      <button @click="closeUploadSuccessDialog()">Close</button>
    </dialog>
  </div>
</template>
<style>
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
  top: 4px;
  margin: 0 20px 0 0;
}

.md-checkbox {
  padding-top: 8px;
}

.spinner {
  margin-top: 16px;
  margin-left: 20px;
}

.stat {
  font-size: 12px;
  margin-right: 16px;
}

.stat strong {
  margin-right: 4px;
}

dialog {
  top: 40vh;
  border-radius: 5px;
  border: 1px solid black;
  text-align: center;
}

dialog + .backdrop {
  background-color: rgba(0, 0, 0, 0.4);
}
</style>
<script>
import Menu from "./Menu";
import Header from "./Header";

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
  },
  data() {
    return {
      containerName: null,
      isContainerPublic: false,
      businessUnitName: null,
      businessUnits: [],
      storageMetadata: [],
      destinationFileNameOptions: [],
      originalFileName: null,
      destinationFileName: null,
      containsPii: false,
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

      this.isContainerPublic = await this.repository._isContainerPublic(
        this.containerName
      );

      this.loading = false;

      this.pipelineStatus = await this.repository._getPipelineStatus(
        this.containerName
      );

      if (this.pipelineStatus == null) {
        this.pipelineStatus = { status: "n/a", message: "", runEnd: "" };
      }
    },
    onFileSelection(event) {
      this.fileBlob = event[0];
      this.originalFileName = event[0].name;
    },
    async uploadFile() {
      this.uploading = true;
      if (!this.fileBlob) {
        this.setUploadErrorState("Please select a file to be uploaded.");
        return;
      }

      if (!this.destinationFileName) {
        this.setUploadErrorState(
          "Please select a destination file name for the upload."
        );
        return;
      }

      const formData = new FormData();
      formData.append("file", this.fileBlob, this.destinationFileName);
      await this.repository._save(
        this.containerName,
        this.containsPii,
        formData
      );
      this.displayUploadSuccessDialog();
      this.uploading = false;

      await this.updateBusinessUnitContent();
    },
    setUploadErrorState(errorMessage) {
      window.alert(errorMessage);
      this.uploading = false;
    },
    async updateContainer(containerName) {
      this.containerName = containerName;
      await this.updateBusinessUnitContent();
      this.$forceUpdate();
    },
    deleteFile(fileName, snapshot) {
      this.repository._deleteBlob(this.containerName, fileName, snapshot);
      this.storageMetadata = this.storageMetadata.filter(
        meta =>
          !(
            meta.containerName === this.containerName &&
            meta.fileName === fileName &&
            meta.snapshot === snapshot
          )
      );
    },
    downloadBlob(blobItem) {
      this.repository._downloadBlob(
        blobItem.containerName,
        blobItem.fileName,
        blobItem.snapshot
      );
    },
    alertPipelineErrorMessage() {
      window.alert(this.pipelineStatus.message);
    },
    closeUploadSuccessDialog() {
      this.$refs["upload-success-dialog"].close();
    },
    displayUploadSuccessDialog() {
      this.$refs["upload-success-dialog"].showModal();
    }
  }
};
</script>
