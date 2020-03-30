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
          <div class="md-layout-item md-size-70">
            <strong>STATUS:</strong>
            <span v-if="injestStatus.isDone === null">n/a</span>
            <span v-else-if="injestStatus.isDone === true">Complete</span>
            <span v-else-if="injestStatus.isDone === false">Processing</span>
          </div>
          <div class="md-layout-item md-size-10">
            <md-button class="md-dense top-btn md-raised md-accent" @click="stopIngest()">Stop</md-button>
          </div>
          <div class="md-layout-item md-size-10">
            <md-button class="md-secondary md-raised top-btn" @click="clearLog()">Clear</md-button>
          </div>
          <div class="md-layout-item md-size-10">
            <md-button class="md-primary md-raised top-btn" @click="refreshLog()">Refresh</md-button>
          </div>
        </div>
        <md-table
          v-if="injestStatus.logItems"
          v-model="injestStatus.logItems"
          md-sort="time"
          md-sort-order="asc"
        >
          <md-table-row slot-scope="{ item }" slot="md-table-row">
            <md-table-cell md-label="Timestamp" md-sort-by="time">{{item.time}}</md-table-cell>
            <md-table-cell md-label="Message">{{item.message}}</md-table-cell>
          </md-table-row>
        </md-table>
      </div>
      <div v-if="loading" class="loading">
        <md-progress-bar md-mode="indeterminate"></md-progress-bar>
      </div>
    </div>
  </div>
</template>
<style>
.status {
  width: 162px;
}
</style>
<script>
import Menu from "./Menu";
import Header from "./Header";

export default {
  name: "Log",
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
      injestStatus: this.defaultInjectStatus()
    };
  },
  methods: {
    async refreshLog() {
      this.loading = true;
      this.injestStatus = await this.repository._getIngestStatus(
        this.containerName
      );
      this.loading = false;
    },
    clearLog() {
      this.repository._clearIngestStatus(this.containerName);
      this.injestStatus.isDone = null;
      this.injestStatus.logItems = [];
    },
    stopIngest() {
      this.repository._stopIngestProcess(this.containerName);
    },
    defaultInjectStatus() {
      return {
        isDone: null,
        logItems: []
      };
    },
    async updateBusinessUnitContent() {
      this.businessUnits = await this.repository._getBusinessUnits();
      this.businessUnitName = this.businessUnits.find(
        b => b.containerName === this.containerName
      ).businessName;
      this.injestStatus = await this.repository._getIngestStatus(
        this.containerName
      );

      if (!this.injestStatus) {
        this.injestStatus = this.defaultInjectStatus();
      }
    },
    async updateContainer(containerName) {
      this.containerName = containerName;
      await this.updateBusinessUnitContent();
      this.$forceUpdate();
    }
  }
};
</script>
