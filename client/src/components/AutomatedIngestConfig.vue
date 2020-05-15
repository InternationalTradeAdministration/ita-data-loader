<template>
  <div>
    <div class="md-layout md-gutter">
      <div class="md-layout-item md-size-70"></div>
      <div class="md-layout-item md-size-30">
        <md-button
          v-if="!editing"
          class="md-secondary md-raised md-dense top-btn"
          @click="editConfiguration()"
        >
          <label>Edit</label>
        </md-button>
        <md-button
          v-if="editing"
          class="md-primary md-raised md-dense top-btn"
          @click="saveConfiguration()"
        >
          <label>Save</label>
        </md-button>
        <md-button
          v-if="!editing"
          class="md-primary md-raised md-dense top-btn"
          @click="startIngestProcess()"
        >
          <label>Ingest</label>
        </md-button>
      </div>
    </div>
    <div class="md-layout md-gutter">
      <div class="md-layout">
        <div class="config-text">
          <json-viewer v-if="!editing && dataSetConfigs" :value="dataSetConfigs" :expand-depth="4" />
          <md-field v-else>
            <textarea v-model="dataSetConfigsBeautified" rows="200" cols="195" wrap="off"></textarea>
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
  </div>
</template>
<script>
import beautify from "json-beautify";

export default {
  name: "AutomatedIngestConfig",
  props: ["repository", "containerName"],
  data: () => ({
    editing: false,
    dataSetConfigs: null,
    dataSetConfigsBeautified: null,
    configSaved: false,
    ingestClicked: false,
    ingestMessage: null
  }),
  async created() {
    let config = await this.repository._getAutomatedIngestConfig(this.containerName).catch(error => {
      console.error(error)
      return {
        dataSetConfigs: []
      }
    });
    this.dataSetConfigs = config.dataSetConfigs;
    this.dataSetConfigsBeautified = beautify(this.dataSetConfigs, null, 2, 100);
  },
  methods: {
    async startIngestProcess() {
      this.ingestClicked = true;
      const status = await this.repository._startAutomatedIngestProcess(
        this.containerName,
        this.dataSetConfigs
      );
      if (status === "started") {
        this.ingestMessage =
          "The ingest process was started successfully! To view its progress, see the log.";
      }
      if (status === "running") {
        this.ingestMessage =
          "The ingest process is currently running! To view its progress, see the log.";
      }
    },
    async saveConfiguration() {
      this.dataSetConfigs = JSON.parse(this.dataSetConfigsBeautified);
      let configSaveResponse = await this.repository._saveAutomatedIngestConfig(
        this.dataSetConfigs,
        this.containerName
      );

      if (configSaveResponse.status === 200) this.configSaved = true;
      this.editing = false;
    },
    editConfiguration() {
      this.editing = true;
    }
  }
};
</script>
<style scoped>
.config-text {
  margin-top: 12px;
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
