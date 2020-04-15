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
        <div v-if="!loading">
          <dataloader-admin-config
            v-if="containerName === 'dataloader'"
            :repository="repository"
          />
          <automated-ingest-config
            v-else
            :repository="repository"
            :containerName="containerName"
          />
        </div>
      </div>
      <div v-if="loading" class="loading">
        <md-progress-bar md-mode="indeterminate"></md-progress-bar>
      </div>
    </div>
  </div>
</template>

<script>
import Menu from "./Menu";
import Header from "./Header";
import DataloaderAdminConfig from "./DataloaderAdminConfig";
import AutomatedIngestConfig from "./AutomatedIngestConfig";

export default {
  name: "Config",
  props: ["repository"],
  components: {
    "dataloader-header": Header,
    "dataloader-menu": Menu,
    "dataloader-admin-config": DataloaderAdminConfig,
    "automated-ingest-config": AutomatedIngestConfig
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
      businessUnits: []
    };
  },
  methods: {
    async updateBusinessUnitContent() {
      this.loading = true;
      this.businessUnits = await this.repository._getBusinessUnits();
      this.businessUnitName = this.businessUnits.find(
        b => b.containerName === this.containerName
      ).businessName;
      this.loading = false;
    },
    async updateContainer(containerName) {
      this.containerName = containerName;
      await this.updateBusinessUnitContent();
      this.$forceUpdate();
    }
  }
};
</script>
