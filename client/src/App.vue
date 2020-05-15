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

.top-btn {
  margin: 16px 32px 0 0;
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
