/**
 * Created by dbenson, dconway on 18/11/2013.
 */

'use strict';

var dao = require('./main.js')

var params = {
   data_api_out_q: {spec:'tcp://127.0.0.1:49994', id: 'data_api'},
   mong_in_q     : {spec:'tcp://127.0.0.1:49996', id:'dao_conn'},
   data_api_in_q : {spec:'tcp://127.0.0.1:49995'},
   logger_params : {
      'path'     : '/opt/openi/cloudlet_platform/logs/dao',
      'log_level': 'debug',
      'as_json'  : false
   }
}


dao(params)