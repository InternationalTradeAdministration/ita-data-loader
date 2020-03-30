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
    <git-hub />
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
</style>
<script>
import GitHub from "./GitHub";
export default {
  name: "Header",
  props: ["initialContainerName", "businessUnits", "updateContainerFn"],
  components: {
    "git-hub": GitHub
  },
  created() {
    this.containerName = this.initialContainerName
  },
  data: () => ({
    containerName: null
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
    }
  }
};
</script>
