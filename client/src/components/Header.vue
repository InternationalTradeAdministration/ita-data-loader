<template>
  <md-toolbar class="md-primary md-dense dataloader-toolbar">
    <div style="flex: 1; display: flex">
      <h3 class="md-title dataloader-title">Dataloader</h3>
      <div v-if="businessUnits && businessUnits.length > 1" class="business-unit-dropdown">
        <select v-model="containerName" @change="updateBusinessUnitContent($event)">
          <option
            v-for="businessUnit in businessUnits"
            :key="businessUnit.containerName"
            :value="businessUnit.containerName"
            :selected="businessUnit.containerName === containerName"
          >{{businessUnit.businessName}}</option>
        </select>
      </div>
      <div v-else-if="businessUnits && businessUnits.length === 1">
        <h3 class="md-title dataloader-title"> - {{businessUnits[0].businessName}}</h3>
      </div>
    </div>
    <span class="version">{{version}}</span>
  </md-toolbar>
</template>
<style>
.business-unit-dropdown {
  margin-left: 20px;
  width: 200px;
}

.business-unit-dropdown select {
  font-size: 16px;
  padding: 4px;
}

.version {
  margin-right: 22px;
}
</style>
<script>
export default {
  name: "Header",
  props: ["initialContainerName", "businessUnits", "updateContainerFn"],
  async created() {
    this.containerName = this.initialContainerName
    this.version = await this.getVersion()
  },
  data: () => ({
    containerName: null,
    version: null
  }),
  methods: {
    updateBusinessUnitContent() {
      this.$router.push({
        name: this.$route.name,
        params: {
          containerName: this.containerName
        }
      });
      this.updateContainerFn(this.containerName)
    },
    async getVersion() {
      let versionResponse = await fetch('/api/version')
      return versionResponse.text()
    }
  }
};
</script>
