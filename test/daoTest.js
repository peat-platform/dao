/**
 * Created by dconway on 08/09/15.
 */
'use strict';

var rewire = require("rewire");


var dao                 = rewire('../lib/main.js');
var helper              = rewire('../lib/CBasehelper.js');
var perms_prop_worker   = rewire('../lib/perms_propogator_worker');
var subscriptionHandler = rewire('../lib/subscriptions');

var couchbase  = require('couchbase').Mock;
var cluster = new couchbase.Cluster();
var bucket = cluster.openBucket();

var assert = require('chai').assert;

helper.__set__({
   "couchbase": require('couchbase').Mock,
   "trackletWorker" : {"send":function(data){return}}
});

var mockcb = function(cd){
   return (function(){
      cd.apply(this,arguments);
   })
};


describe('Test Main',function(){
   var config = {
      dao_sink           : {spec:'tcp://127.0.0.1:49997', bind:false, id:'q1', type:'pull' },
      sub_sink           : {spec:'tcp://127.0.0.1:49500', bind:false, id:'subpush', type:'pub' },
      tracklet_worker    : {spec:'tcp://127.0.0.1:49502', bind:false, id:'tracklet', type:'push' },
      perms_propagator_f : {spec:'tcp://127.0.0.1:49700', bind:false, id:'perms_f', type:'push' },
      perms_propagator_b : {spec:'tcp://127.0.0.1:49701', bind:false, id:'perms_b', type:'pull' },
      logging : {
         'name'          : 'dao',
         'log_level'     : 'info',
         'log_file_name' : './dao',
         'as_json'       : false
      }
   };
   var invalidConfig = {
      dao_sink        : { spec:'tcp://127.0.0.1:49999', bind:false, type:'left', id:'a'},
      mongrel_handler : {
         source : { spec:'tcp://127.0.0.1:49905', bind:false, id:'b', type:'pull', isMongrel2:true },
         sink   : { spec:'tcp://127.0.0.1:49906', bind:false, id:'c', type:'pub', isMongrel2:true}
      },
      logger_params : {
         'name'          : 'object_api',
         'log_level'     : 'info',
         'log_file_name' : './object_api',
         'as_json'       : false
      }
   };
   it('should pass init with config', function () {
      try{
         dao(config);
      } catch(e){
         console.log("should pass init with config: ",e);
      }
   });
   it('should throw error with bad config', function () {
      try{
         dao(invalidConfig);
      }catch(e){
         //console.log("should throw error with bad config: ",e);
         assert.isNotNull(e,"Error should be thrown");
      }
   });
});

describe('Test Helper',function(){

   var sessionToken = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJqdGkiOiJtb2NoYVRlc3RfM2Q0MTk5ZWMtYjE0Ni00Y2IzLThmYWItN2MzY2M4ZjMwYzllIiwiaXNzIjoiaHR0cHM6Ly8xMjcuMC4wLjE6MTM0My9hdXRoL3Rva2VuIiwic3ViIjoibW9jaGFUZXN0IiwiZXhwIjoxNDQxNzU5ODA4LCJpYXQiOjE0NDE3MTY2MDgsIm5vbmNlIjoiMmY0MzA3YWMtY2FmZC00NmI0LTg4OWUtNDExNzBmZjRkMTg0IiwidXNlcl9pZCI6Im1vY2hhVGVzdCIsImNsb3VkbGV0IjoiY180MGRlMmM2NTRmNzcyMzg3YTY4MTNhZTE0Y2RkNmM3YiIsInNjb3BlIjoicGVhdCIsInBlYXQtdG9rZW4tdHlwZSI6InNlc3Npb24iLCJyZXNwb25zZV90eXBlIjoiaWRfdG9rZW4ifQ.aXSaDAzKpmjgoV_utoOokyMiMGq0QK3IRfiJPvgpKIOrKg_qr-rXcjMb_-eA1_N6Q3sFHSRBv-hPTZbEXnX7iBMSVN1zdz9QyESGBOjawjsUFhtn1FsxFnrsmgyPZljn19JddZItrDFxRDKmkbKmMVeWu6LGsXiFwIO2Fg_5Bm3rAXi8-XaL7BUU0UvmwKbJYrxGWCETGWg22SU13kufDxx0Fo-LT0wpWuQZSltb_CcODl4QmbK00mSOS9umlEYX9PF5mOu0zAmEcgXqqcGgk2Bvlne_I0O_gR7BKL6YiGuHmDVgFPInvLAeGFdLUS8jnUpzVcViDmkq8t1F-TOLlQ'
   var authToken = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJqdGkiOiJjXzQwZGUyYzY1NGY3NzIzODdhNjgxM2FlMTRjZGQ2YzdiXzQ4MGQ4M2JmLTk3ZGYtNGRiOS04ZDNmLTg2YTY2MTM5ODFlMCIsImlzcyI6Imh0dHBzOi8vMTI3LjAuMC4xOjEzNDMvYXV0aC90b2tlbiIsInN1YiI6ImNfNDBkZTJjNjU0Zjc3MjM4N2E2ODEzYWUxNGNkZDZjN2IiLCJleHAiOjE0NDE3NjAwMTksImlhdCI6MTQ0MTcxNjgxOSwibm9uY2UiOiI1Y2I2ZGZhMC1iYzYwLTQ3MzYtOWRmOC1lYjMwZDYyOTY1OGQiLCJ1c2VyX2lkIjoiY180MGRlMmM2NTRmNzcyMzg3YTY4MTNhZTE0Y2RkNmM3YiIsImNsb3VkbGV0IjoiY180MGRlMmM2NTRmNzcyMzg3YTY4MTNhZTE0Y2RkNmM3YiIsImNsaWVudF9pZCI6IjNjNzhmZTY2ZDc3YzVjYmUyYmRlZjEzZWZlMTBlM2MzIiwiY2xpZW50X25hbWUiOiJtb2NoYVRlc3QiLCJjb250ZXh0IjoiY180MGRlMmM2NTRmNzcyMzg3YTY4MTNhZTE0Y2RkNmM3YiIsInNjb3BlIjoicGVhdCIsInBlYXQtdG9rZW4tdHlwZSI6InRva2VuIiwicmVzcG9uc2VfdHlwZSI6ImlkX3Rva2VuIn0.qf73YVDAXDpcsPbNRJJgTKqdh0fxC5Tpm8lw52U3t72fSeazJSS1uOIQGGxQvSpYS8bwdPjJyt9u8xGdbzBBKsGtuhtTw-TEVaPQLl1IzBwNImvfCUdZjaV-09AnTnppeO5o8diBqQ2hOA07oI4Zn9pFgkkur4YxufIAzO_Bj9Ny5Sg0L-DlXV3Q3lx30gS3hs9Ii72fKSjn7zjkoGZ2-UEs2hGnlsS57AtdfdUvTx9N3F5uTpWniIcjx6Nss7yRitMeB6kjELCZinJbAM3l3UaWoP0_pzVw0IMg8Sa8CwssilG1MoCPxRebJ7YG03OViIX7G8xCdn65EHNqYXTVxg'
   var validType = {
      "@type": "t_e18dd069371d528764d51c54d5bf9611-167",
      "@data": {
         "stringArray": [
            "mock stringArray 1",
            "mock stringArray 2",
            "mock stringArray 3"
         ]
      }
   };

   var daoActionTemplate = {
      'dao_actions'      : [],
      'mongrel_sink' : "terminal_handler",
      'clients'      : [
         {
            'uuid'   : "msg.uuid",
            'connId' : "msg.connId"
         }
      ]
   };

   var actionGetObjectId = [
      {
         'action'      : 'GET',
         'database'    : 'c_897b0ef002da79321dcb0d681cb473d0+0b1d1210-283c-407d-87d9-b88cf218379a',
         'resolve'     : false,
         'resp_type'   : 'object',
         'property'    : '',
         'meta'        : undefined,
         'api_key'     : '86eaec8f28be20cee4aa3d05ea6a1ae0',
         'third_party' : 'c_897b0ef002da79321dcb0d681cb473d0',
         'bucket'      : 'objects',
         'client_name' :  undefined,
         'third_party_cloudlet' :  undefined,
         'headers'     : {
            'x-forwarded-for' : "127.0.0.1",
            'REMOTE_ADDR'     : "127.0.0.1"
         }
      }
   ];

   var exampleObject = {
      "@id": "014dd88f-36f2-42a0-9e4e-a5dfc02060ef",
      "@location": "/api/v1/objects/c_897b0ef002da79321dcb0d681cb473d0/0b1d1210-283c-407d-87d9-b88cf218379a",
      "@cloudlet": "c_897b0ef002da79321dcb0d681cb473d0",
      "@type": "t_670a968a4a7bbe6fbbd434d23ed756c0-1667",
      "@type_location": "/api/v1/types/t_670a968a4a7bbe6fbbd434d23ed756c0-1667",
      "@data": {
         "street": "Carriganore",
         "town": "Waterford",
         "houseName": "ArcLabs",
         "hasHome": true,
         "children": 2,
         "title": "Mr",
         "dateOfBirth": "1990-06-01",
         "telephone": 863291617,
         "lastname": "Dev",
         "email": "margadh.tssg@gmail.com",
         "county": "Waterford",
         "gender": "Male",
         "firstname": "Wizeoni"
      },
      "_date_created": "2015-06-09T13:42:54.133Z",
      "_date_modified": "2015-06-09T13:42:54.133Z",
      "_permissions": {
         "created_by": "c_897b0ef002da79321dcb0d681cb473d0",
         "created_by_app": "86eaec8f28be20cee4aa3d05ea6a1ae0",
         "c_897b0ef002da79321dcb0d681cb473d0": {
            "create": true,
            "read": true,
            "update": true,
            "delete": true
         },
         "86eaec8f28be20cee4aa3d05ea6a1ae0": {
            "create": true,
            "read": true,
            "update": true,
            "delete": true
         }
      }
   }

   helper.__set__("dbs",{'objects':bucket,'types':bucket,'attachments':bucket,'permissions':bucket,'app_permissions':bucket});


   it("Should try get object",function(){
      var cb = function(err, results, httpStatusCode){
         //console.log(results);
         assert.isNull(err, "Error should be null");
         assert.isNotNull(results,"results should not be null");
         assert(results['0'][0]['@data'] == exampleObject['@data'],'Object Should match Example')
      };

      bucket.upsert('c_897b0ef002da79321dcb0d681cb473d0+0b1d1210-283c-407d-87d9-b88cf218379a',exampleObject, function(err, result) {
         if (err) throw err;
      });

      var getAction = daoActionTemplate;
      getAction.dao_actions=actionGetObjectId;

      try {
         helper.evaluateMessage(getAction, mockcb(cb))
      }catch(e){
         console.log("Should try get object: ",e)
      }
   })



});
