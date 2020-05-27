<template>
  <nav class="nav-menu">
    <ul>
      <li :class="getClasses('Upload')" @click="goTo('Upload')">
        <md-icon id="menu-icon">arrow_upward</md-icon>
        <span>Upload</span>
      </li>
      <li :class="getClasses('Config')" @click="goTo('Config')">
        <md-icon id="menu-icon">settings_applications</md-icon>
        <span>Configure</span>
      </li>
      <li :class="getClasses('SystemLogs')" @click="goTo('SystemLogs')">
        <md-icon id="menu-icon">menu_book</md-icon>
        <span>Log</span>
      </li>
    </ul>
  </nav>
</template>
<style scoped>
ul {
  list-style-type: none;
  padding: 0;
  margin: 0;
}

li {
  background: #8b9197;
  color: white;
  margin-left: 10px;
  cursor: pointer;
  margin: 0;
  padding-left: 14px;
  display: flex;
  padding-top: 10px;
  height: 46px;
}

.nav-menu {
  background: #8b9197;
  height: 100vh;
  width: 55px;
}

.nav-menu:hover {
  transition: 0.5s;
  width: 156px;
}

#menu-icon {
  color: white;
  margin: 0 16px 0 0;
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
