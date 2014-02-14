/**
 * Created by dbenson, dconway on 18/11/2013.
 */

'use strict';

var dao = require('./main.js')

var config = {
   dao_sink       : {spec:'tcp://127.0.0.1:49999', bind:true, id:'q1', type:'pull' },
   logger_params  : {
      'path'      : '/opt/openi/cloudlet_platform/logs/dao',
      'log_level' : 'debug',
      'as_json'   : true
   }
}


dao(config)