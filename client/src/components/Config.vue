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
          <div class="md-layout-item md-size-80"></div>
          <div class="md-layout-item md-size-10">
            <md-button
              v-if="!editing"
              class="md-secondary md-raised top-btn"
              @click="editConfiguration()"
            >
              <label>Edit</label>
            </md-button>
            <md-button
              v-if="editing"
              class="md-secondary md-raised top-btn"
              @click="saveConfiguration()"
            >
              <label>Save</label>
            </md-button>
          </div>
          <div class="md-layout-item md-size-10">
            <md-button
              class="md-primary md-raised top-btn"
              @click="startIngestProcess()"
            >
              <label>Ingest</label>
            </md-button>
          </div>
        </div>
        <div class="md-layout md-gutter">
          <div class="md-layout">
            <div class="config-text">
              <json-viewer v-if="!editing" :value="dataloaderConfig" :expand-depth=4 />
              <md-field v-else>
                <textarea v-model="dataloaderConfigBeautified" rows="200" cols="195" wrap="off"></textarea>
              </md-field>
            </div>
            <md-dialog-alert
              :md-active.sync="configSaved"
              md-content="The configuration was saved successfully! "
              md-confirm-text="Close"
            />
            <md-dialog-alert
              :md-active.sync="ingestClicked"
              :md-content="ingestMessage"
              md-confirm-text="Close"
            />
          </div>
        </div>
        <div v-if="loading" class="loading">
          <md-progress-bar md-mode="indeterminate"></md-progress-bar>
        </div>
      </div>
    </div>
  </div>
</template>
<style>
.config-text {
  margin-left: 35px;
  width: 1350px;
}
.json-key {
  color: brown;
}
.json-value {
  color: navy;
}
.json-boolean {
  color: teal;
}
.json-string {
  color: olive;
}
</style>
<script>
import Menu from "./Menu";
import Header from "./Header";
import beautify from "json-beautify";

export default {
  name: "Config",
  props: ["repository"],
  components: {
    "dataloader-header": Header,
    "dataloader-menu": Menu
  },
  async created() {
    this.loading = true;
    this.containerName = this.$route.params["containerName"];
    await this.updateBusinessUnitContent();
    this.loading = false;
  },
  data() {
    return {
      loading: true,
      containerName: null,
      businessUnits: [],
      dataloaderConfig: {},
      configSaved: false,
      ingestClicked: false,
      ingestMessage: null,
      editing: false,
      dataloaderConfigBeautified: null
    };
  },
  methods: {
    async startIngestProcess() {
      this.ingestClicked = true;
      const status = await this.repository._startIngestProcess(this.containerName);
      if (status === 'started') {
        this.ingestMessage = 'The ingest process was started successfully! To view its progress, see the log.'
      }
      if (status === 'running') {
        this.ingestMessage = 'The ingest process is currently running! To view its progress, see the log.'
      }
    },
    async saveConfiguration() {
      this.dataloaderConfig = JSON.parse(this.dataloaderConfigBeautified);
      let configSaveResponse = await this.repository._saveDataloaderConfig(
        this.dataloaderConfig,
        this.containerName
      );

      if (configSaveResponse.status === 200) this.configSaved = true;
      this.editing = false;
    },
    editConfiguration() {
      this.editing = true;
    },
    async updateBusinessUnitContent() {
      this.dataloaderConfig = await this.repository._getDataloaderConfig(
        this.containerName
      );
      this.dataloaderConfigBeautified = beautify(
        this.dataloaderConfig,
        null,
        2,
        100
      );
      this.businessUnits = await this.repository._getBusinessUnits();
      this.businessUnitName = this.businessUnits.find(
        b => b.containerName === this.containerName
      ).businessName;
    },
    async updateContainer(containerName) {
      this.containerName = containerName;
      await this.updateBusinessUnitContent();
      this.$forceUpdate()
    }
  }
};
</script>
