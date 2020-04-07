import Router from 'vue-router'
import Upload from '@/components/Upload'
import Config from '@/components/Config'
import Log from '@/components/Log'
import APIs from '@/components/APIs'

export default function router (repository) {
  return new Router({
    routes: [
      {
        path: '/upload/:containerName',
        name: 'Upload',
        component: Upload,
        props: {
          repository
        }
      }, {
        path: '/',
        name: 'Home',
        component: Upload,
        props: {
          repository
        }
      }, {
        path: '/config/:containerName',
        name: 'Config',
        component: Config,
        props: {
          repository
        }
      }, {
        path: '/log/:containerName',
        name: 'Log',
        component: Log,
        props: {
          repository
        }
      }, {
        path: '/apis/:containerName',
        name: 'APIs',
        component: APIs
      }
    ]
  })
}
