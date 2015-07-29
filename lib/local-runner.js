/**
 * Created by dbenson, dconway on 18/11/2013.
 */

'use strict';

var dao = require('./main.js');

var config = {
   dao_sink           : {spec:'tcp://127.0.0.1:49997', bind:false, id:'q1', type:'pull' },
   sub_sink           : {spec:'tcp://127.0.0.1:49500', bind:false, id:'subpush', type:'pub' },
   tracklet_worker    : {spec:'tcp://127.0.0.1:49502', bind:false, id:'tracklet', type:'push' },
   perms_propagator_f : {spec:'tcp://127.0.0.1:49700', bind:false, id:'perms_f', type:'push' },
   perms_propagator_b : {spec:'tcp://127.0.0.1:49701', bind:false, id:'perms_b', type:'pull' },
   logger_params  : {
      'path'      : '/opt/peat/cloudlet_platform/logs/dao',
      'log_level' : 'debug',
      'as_json'   : true
   }
};


dao(config);