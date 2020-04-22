<template>
  <nav class="nav-menu">
    <md-list class="menu-list-items">
      <md-list-item :class="getClasses('Upload')" @click="goTo('Upload')">
        <md-icon id="menu-icon">arrow_upward</md-icon>
        <span class="md-list-item-text">Upload</span>
      </md-list-item>
      <md-list-item :class="getClasses('Config')" @click="goTo('Config')">
        <md-icon id="menu-icon">settings_applications</md-icon>
        <span class="md-list-item-text">Configure</span>
      </md-list-item>
      <md-list-item :class="getClasses('SystemLogs')" @click="goTo('SystemLogs')">
        <md-icon id="menu-icon">menu_book</md-icon>
        <span class="md-list-item-text">Log</span>
      </md-list-item>
      <md-list-item :class="getClasses('APIs')" @click="goTo('APIs')">
        <md-icon id="menu-icon">developer_mode</md-icon>
        <span class="md-list-item-text">APIs</span>
      </md-list-item>
    </md-list>
  </nav>
</template>
<style>
.nav-menu {
  background: #8b9197;
  height: 100vh;
  width: 55px;
}

.nav-menu:hover {
  transition: 0.5s;
  width: 156px;
}

.menu-list-items {
  padding: 0;
}

.menu-li {
  background: #8b9197;
}

.md-list-item-text {
  color: white;
  margin-left: 10px;
}

#menu-icon {
  color: white;
  margin: 0 5px 0 0;
}

.menu-li.current-route {
  background: #ff3333;
}
</style>
<script>
export default {
  name: "Menu",
  props: {
    containerName: String
  },
  methods: {
    goTo(routeName) {
      this.$router.push({
        name: routeName,
        params: {
          containerName: this.containerName
        }
      });
    },
    getClasses(routeName) {
      if (routeName === 'Upload' && this.$route.name === 'Home')
        return "menu-li current-route"
      else
        return routeName === this.$route.name
          ? "menu-li current-route"
          : "menu-li";
    }
  }
};
</script>
