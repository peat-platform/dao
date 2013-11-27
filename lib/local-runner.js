/**
 * Created by dbenson, dconway on 18/11/2013.
 */

'use strict';

var dao = require('./main.js')

var params = {
   dao_sub_q      : {spec:'tcp://127.0.0.1:49994', id:'dao_sub_q_dao_1'    },
   mongrel_sub_q  : {spec:'tcp://127.0.0.1:49996', id:'mongrel_sub_q_dao_1'},
   logger_params  : {
      'path'      : '/opt/openi/cloudlet_platform/logs/dao',
      'log_level' : 'debug',
      'as_json'   : true
   }
}


dao(params)