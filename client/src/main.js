// The Vue build version to load with the `import` command
// (runtime-only or standalone) has been set in webpack.base.conf with an alias.
import Vue from 'vue'
import App from './App'
import Repository from '@/utils/Repository'
import Router from 'vue-router'
import router from './router'
import VueMaterial from 'vue-material'
import JsonViewer from 'vue-json-viewer'
import 'vue-material/dist/vue-material.min.css'

Vue.config.productionTip = false

Vue.use(Router)
Vue.use(VueMaterial)
Vue.use(JsonViewer)

const repository = new Repository()
new Vue({
  router: router(repository),
  render: h => h(App, { props: { repository } })
}).$mount('#app')
