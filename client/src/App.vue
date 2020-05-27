<template>
  <div id="container">
    <router-view />
  </div>
</template>

<script>
export default {
  name: "App"
};
</script>

<style lang="scss">
@import "~vue-material/dist/theme/engine";
@include md-register-theme("default", (
    primary: #3e4853
));
@import "~vue-material/dist/theme/all";

.top-btn.md-button {
  margin: 16px 22px 0 12px;
}

.loading {
  margin-left: 30px;
  margin-top: 16px;
}

.nav-menu {
  height: 500px;
  margin-right: 16px;
}

.content {
  display: flex;
}

.sub-content {
  min-width:  1000px;
  width: 100%;
  padding-top: 12px;
}

</style>
