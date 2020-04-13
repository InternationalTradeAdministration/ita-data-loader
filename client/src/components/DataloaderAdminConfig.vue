<template>
  <div>
    <div class="md-layout md-gutter">
      <div class="md-layout-item md-size-85"></div>
      <div class="md-layout-item md-size-15">
        <md-button class="md-primary md-raised md-dense top-btn" @click="saveConfiguration()">
          <label>Save</label>
        </md-button>
      </div>
    </div>
    <div class="md-layout md-gutter">
      <div class="md-layout-item md-size-90">
        <md-table
          v-model="businessUnits"
          md-sort="containerName"
          md-sort-order="asc"
          class="business-units"
        >
          <md-table-row slot-scope="{ item }" slot="md-table-row">
            <md-table-cell md-label="Business Unit" md-sort-by="businessName">
              <md-field>
                <md-input v-model="item.businessName" class="business-name" />
              </md-field>
            </md-table-cell>
            <md-table-cell
              md-label="Storage Container"
              md-sort-by="containerName"
            >{{item.containerName}}</md-table-cell>
            <md-table-cell md-label="Users">
              <md-chips v-model="item.users" md-placeholder="Add user..."></md-chips>
            </md-table-cell>
          </md-table-row>
        </md-table>
        <md-dialog-alert
          :md-active.sync="configSaved"
          md-content="The configuration was saved successfully! "
          md-confirm-text="Close"
        />
      </div>
    </div>
  </div>
</template>
<script>
import beautify from "json-beautify";
import Repository from "@/utils/Repository";

export default {
  name: "DataloaderAdminConfig",
  props: ["value", "repository"],
  data: () => ({
    editing: false,
    businessUnits: null,
    configSaved: false
  }),
  created() {
    this.businessUnits = this.value.businessUnits;
  },
  methods: {
    async saveConfiguration() {
      let configSaveResponse = await this.repository._saveDataloaderAdminConfig(
        this.businessUnits
      );

      this.configSaved = true;
    }
  }
};
</script>
<style scoped>
.business-units {
  margin-top: 16px;
  margin-left: 16px;
}
.business-name {
  width: 222px;
}
</style>
