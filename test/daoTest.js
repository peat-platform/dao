/**
 * Created by dconway on 08/09/15.
 */
'use strict';

var rewire = require("rewire");


var dao                 = rewire('../lib/main.js');
var helper              = rewire('../lib/CBasehelper.js');
var perms_prop_worker   = rewire('../lib/perms_propogator_worker');
var subscriptionHandler = rewire('../lib/subscriptions');

var utils   = require('cloudlet-utils').cloudlet_utils;

var couchbase  = rewire('couchbase').Mock;
var cluster = new couchbase.Cluster();
var bucket = cluster.openBucket();
var bucketMngr = bucket.manager();

var assert = require('chai').assert;

helper.__set__({
   "couchbase": require('couchbase').Mock,
   "trackletWorker" : {"send":function(data){return}},
   "logger" : {
      "error":function(err){return},
      "debug":function(err){return},
      "info":function(err){return}
   },
   "cluster" : cluster,
   "bucket" : bucket
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
   };

   helper.__set__("dbs",{
      'objects'         :bucket,
      'types'           :bucket,
      'attachments'     :bucket,
      'permissions'     :bucket,
      'app_permissions' :bucket,
      'users'           :bucket,
      'dbkeys'          :bucket,
      'clients'         :bucket
   });


   describe('Test Object POST',function() {
      var actionPostObjectId = [
         {
            'action'      : 'POST',
            'database'    : 'c_897b0ef002da79321dcb0d681cb473d0+014dd88f-36f2-42a0-9e4e-a5dfc02060ef',
            'object_name' : '014dd88f-36f2-42a0-9e4e-a5dfc02060ef',
            'object_data' : exampleObject,
            'id'          : '014dd88f-36f2-42a0-9e4e-a5dfc02060ef',
            'cloudlet_id' : 'c_897b0ef002da79321dcb0d681cb473d0',
            'api_key'     : '86eaec8f28be20cee4aa3d05ea6a1ae0',
            'third_party' : 'c_897b0ef002da79321dcb0d681cb473d0',
            'bucket'      : 'objects'
         }
      ];
      it("Should try post object", function (done) {
         var cb = function (err, results, httpStatusCode) {
            assert.isNull(err, "Error should be null");
            assert.isTrue(utils.isObjectId(results['@id']), 'Object Id Should not be null');
            assert.equal(201,httpStatusCode, "Status should be 201");

            var doc = 'c_897b0ef002da79321dcb0d681cb473d0+'+results['@id'];

            bucket.get(doc, function (err, result) {
               assert.isNull(err, "Error should be null");
               assert.isNotNull(result, "results should not be null");
               assert.deepEqual(result['value']['@data'], exampleObject['@data'], 'Posted Object should match Example');
               done()
            });
         };

         var postAction = daoActionTemplate;
         postAction.dao_actions = actionPostObjectId;

         try {
            helper.evaluateMessage(postAction, mockcb(cb))
         } catch ( e ) {
            console.log("Should try post object: ", e)
         }
      })
   });

   //describe('Test Object PUT',function() {
   //   var actionPutObjectId = [
   //      {
   //         'action'       : 'PUT',
   //         'database'     : 'c_897b0ef002da79321dcb0d681cb473d0+0b1d1210-283c-407d-87d9-b88cf218379a',
   //         'revision'     : '1',
   //         'object_data'  : exampleObject,
   //         'id'           : '0b1d1210-283c-407d-87d9-b88cf218379a',
   //         'bucket'      : 'objects'
   //      }
   //   ];
   //   it("Should try get object", function (done) {
   //      /* This Test Fails */
   //      var cb = function (err, results, httpStatusCode) {
   //         done()
   //      };
   //
   //      bucket.upsert('c_897b0ef002da79321dcb0d681cb473d0+0b1d1210-283c-407d-87d9-b88cf218379a', exampleObject, function (err, result) {
   //         if ( err ) throw err;
   //      });
   //
   //      var putAction = daoActionTemplate;
   //      putAction.dao_actions = actionPutObjectId;
   //
   //      try {
   //         helper.evaluateMessage(putAction, mockcb(cb))
   //      } catch ( e ) {
   //         console.log("Should try put object: ", e)
   //         done()
   //      }
   //   })
   //});

   describe('Test Object GET',function() {
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
      it("Should try get object", function (done) {
         var cb = function (err, results, httpStatusCode) {
            assert.isNull(err, "Error should be null");
            assert.isNotNull(results, "results should not be null");
            assert.equal(200,httpStatusCode, "Status should be 200");
            assert.deepEqual(results['@data'], exampleObject['@data'], 'Received Object Should match Example');
            done()
         };

         bucket.upsert('c_897b0ef002da79321dcb0d681cb473d0+0b1d1210-283c-407d-87d9-b88cf218379a', exampleObject, function (err, result) {
            if ( err ) throw err;
         });

         var getAction = daoActionTemplate;
         getAction.dao_actions = actionGetObjectId;

         try {
            helper.evaluateMessage(getAction, mockcb(cb))
         } catch ( e ) {
            console.log("Should try get object: ", e)
         }
      })
   });

   describe('Test Object DELETE',function() {
      var actionDeleteObjectId = [
         {
            'action'      : 'DELETE',
            'database'    : 'c_897b0ef002da79321dcb0d681cb473d0+0b1d1210-283c-407d-87d9-b88cf218379a',
            'resolve'     : false,
            'resp_type'   : 'object',
            'api_key'     : '86eaec8f28be20cee4aa3d05ea6a1ae0',
            'third_party' : 'c_897b0ef002da79321dcb0d681cb473d0',
            'bucket'      : 'objects',
            'headers'     : {
               'x-forwarded-for' : "127.0.0.1",
               'REMOTE_ADDR'     : "127.0.0.1"
            }
         }
      ];
      it("Should try get object", function (done) {
         var cb = function (err, results, httpStatusCode) {
            //console.log(err, results, httpStatusCode);
            assert.isNull(err, "Error should be null");
            assert.isNotNull(results, "results should not be null");
            assert.equal(200,httpStatusCode, "Status should be 200");
            done()
         };

         bucket.upsert('c_897b0ef002da79321dcb0d681cb473d0+0b1d1210-283c-407d-87d9-b88cf218379a', exampleObject, function (err, result) {
            if ( err ) throw err;
         });

         var deleteAction = daoActionTemplate;
         deleteAction.dao_actions = actionDeleteObjectId;

         try {
            helper.evaluateMessage(deleteAction, mockcb(cb))
         } catch ( e ) {
            console.log("Should try get object: ", e)
         }
      })
   });

   describe('Test Object VIEW',function() {
      var actionView = [
         {
            'action'      : 'VIEW',
            'design_doc'  : 'objects_views',
            'view_name'   : 'object_by_cloudlet_id',
            'meta'        : {
               "limit"       : 30,
               "offset"      : 0,
               "total_count" : 0,
               "prev"        : null,
               "next"        : 30
            },
            'filter_show' : {},
            'resp_type'   : 'cloudlet',
            'start_key'   : ['c_897b0ef002da79321dcb0d681cb473d0'],
            'end_key'     : ['c_897b0ef002da79321dcb0d681cb473d0' + "^" ],
            'group'       : false,
            'group_level' : 2,
            'reduce'      : true,
            'id_only'     : false,
            'bucket'      : 'objects',
            'headers'     : {
               'x-forwarded-for' : "127.0.0.1",
               'REMOTE_ADDR'     : "127.0.0.1"
            }
         }
      ];
      it("Should try get object", function (done) {
         var cb = function (err, results, httpStatusCode) {
            assert.isNull(err, "Error should be null");
            assert.isNotNull(results, "results should not be null");
            assert.equal(1,results['result'].length, "Length should be 1");
            assert.equal(200,httpStatusCode, "Status should be 200");
            done()
         };

         bucket.upsert('c_897b0ef002da79321dcb0d681cb473d0+0b1d1210-283c-407d-87d9-b88cf218379a', exampleObject, function (err, result) {
            if ( err ) throw err;
         });

         var objects_views = {"views": {
               "object_by_cloudlet_id": { "map": "function (doc, meta) {\n\n  if (undefined === doc[\"@type\"]){    return   }\n\n  var ts = new Date(doc[\"_date_modified\"]).getTime()\n\n  emit( [doc[\"@cloudlet\"], doc[\"@cloudlet\"], ts], doc[\"@id\"] );\n\n  for ( i in doc._permissions){\n\n    if ( doc._permissions[i][\"read\"] ){\n emit( [i, doc[\"@cloudlet\"], ts], doc[\"@id\"] );\n    }\n  }\n}", "reduce":"_count" },
               "object_by_type" : { "map" : "function (doc, meta) {\n if (undefined === doc[\"@type\"]){\n return \n }\n var ts = new Date(doc[\"_date_modified\"]).getTime() \n emit( [doc[\"@cloudlet\"], doc[\"@type\"], doc[\"@cloudlet\"], ts], [doc[\"@cloudlet\"], doc[\"@id\"]] ); \n for ( i in doc._permissions){ \n if ( doc._permissions[i][\"read\"] ){ \n emit( [i, doc[\"@type\"], doc[\"@cloudlet\"], ts], [doc[\"@cloudlet\"], doc[\"@id\"]] );\n }\n}\n}", "reduce" : "_count" },
               "type_usage" : { "map" : "function (doc, meta) {\n if (undefined === doc[\"@type\"]){\n    return \n  }\n emit(doc[\"@type\"], 1);\n}", "reduce" : "_count" },
               "object_data" : { "map" : "function (doc, meta) {\n if (undefined === doc[\"@type\"]){\n    return \n  }\n emit(doc[\"@id\"], doc[\"@type\"]);\n}" },
               "object_by_third_party_type" : { "map" : "function (doc, meta) {if (undefined === doc[\"@type\"]){ return }\n \n emit( [doc[\"@cloudlet\"], doc[\"@cloudlet\"], doc[\"@type\"]], doc[\"@type\"] );\n \n for ( i in doc._permissions){\n \n if ( doc._permissions[i][\"read\"] ){\n emit( [i, doc[\"@cloudlet\"], doc[\"@type\"]], doc[\"@type\"] );\n }\n }\n }", "reduce" : "_count" }
            }
         };

         bucketMngr.upsertDesignDocument("objects_views",objects_views,function (err, result) {
            assert.isNull(err, "Error should be null");
            if ( err ) throw err;
            bucket._indexView("objects_views","object_by_cloudlet_id",null,function(err){
               assert.isNull(err, "Error should be null");
               if ( err ) throw err;

               var viewAction = daoActionTemplate;
               viewAction.dao_actions = actionView;

               try {
                  helper.evaluateMessage(viewAction, mockcb(cb))
               } catch ( e ) {
                  console.log("Should try get object: ", e)
               }
            })
         });
      })
   });

   describe('Test CRUD GENERIC_DELETE',function() {
      var actionGenDeleteId = [
         {
            'action'       : 'GENERIC_DELETE',
            'database'     : 'users',
            'id'           : 'users_devUser',
            'data'         : {},
            'authorization': 'dbkeys_29f81fe0-3097-4e39-975f-50c4bf8698c7',
            'options'      : {}
         }
      ];

      it("Should try crud delete", function (done) {
         var cb = function (err, results, httpStatusCode) {
            assert.isNull(err, "Error should be null");
            assert.isNotNull(results, "results should not be null");
            assert.equal(200,httpStatusCode, "Status should be 200");
            assert.equal(results['request']['id'], actionGenDeleteId[0]['id'], "IDs should match")
            done()
         };

         bucket.upsert('dbkeys_29f81fe0-3097-4e39-975f-50c4bf8698c7', {"dbs": ["users","clients","authorizations","queries"]}, function (err, result) {
            if ( err ) throw err;
         });
         bucket.upsert('users_devUser', {"user": "TestUser"}, function (err, result) {
            if ( err ) throw err;
         });

         var crudDelete = daoActionTemplate;
         crudDelete.dao_actions = actionGenDeleteId;

         try {
            helper.evaluateMessage(crudDelete, mockcb(cb))
         } catch ( e ) {
            console.log("Should try get object: ", e)
         }
      })
   });

   describe('Test CRUD GENERIC_CREATE',function() {
      var actionGenCreateId = [
         {
            'action'       : 'GENERIC_CREATE',
            'database'     : 'users',
            'id'           : 'users_devUser',
            'data'         : { username: 'users_devUser',
               password: 'users_devUser',
               scope: 'user',
               cloudlet: 'c_1bb237a4556baf0dd99d7d4a2f9f2df0' },
            'authorization': 'dbkeys_29f81fe0-3097-4e39-975f-50c4bf8698c7',
            'options'      : {}
         }
      ];

      it("Should try crud create", function (done) {
         var cb = function (err, results, httpStatusCode) {
            //console.log(err, results, httpStatusCode);
            assert.isNull(err, "Error should be null");
            assert.isNotNull(results, "results should not be null");
            assert.equal(201,httpStatusCode, "Status should be 201");
            assert.equal(results['request']['id'], actionGenCreateId[0]['id'], "IDs should match");
            assert.equal(results['request']['data']['username'], results['response']['username'], "Usernames Shout Match");
            done()
         };

         bucket.upsert('dbkeys_29f81fe0-3097-4e39-975f-50c4bf8698c7', {"dbs": ["users","clients","authorizations","queries"]}, function (err, result) {
            if ( err ) throw err;
         });

         var crudCreate = daoActionTemplate;
         crudCreate.dao_actions = actionGenCreateId;

         try {
            helper.evaluateMessage(crudCreate, mockcb(cb))
         } catch ( e ) {
            console.log("Should try get object: ", e)
         }
      })
   });

   describe('Test CRUD GENERIC_READ',function() {
      var actionGenReadId = [
         {
            'action'       : 'GENERIC_READ',
            'database'     : 'users',
            'id'           : 'users_devUser2',
            'data'         : {},
            'authorization': 'dbkeys_29f81fe0-3097-4e39-975f-50c4bf8698c7',
            'options'      : {}
         }
      ];

      var user = {username: 'users_devUser',password: 'users_devUser',scope: 'user', cloudlet: 'c_1bb237a4556baf0dd99d7d4a2f9f2df0'}

      it("Should try crud read", function (done) {
         var cb = function (err, results, httpStatusCode) {
            //console.log(err, results, httpStatusCode);
            assert.isNull(err, "Error should be null");
            assert.isNotNull(results, "results should not be null");
            assert.equal(200,httpStatusCode, "Status should be 201");
            assert.equal(results['response']['username'], user['username'], "IDs should match")
            done()
         };

         bucket.upsert('dbkeys_29f81fe0-3097-4e39-975f-50c4bf8698c7', {"dbs": ["users","clients","authorizations","queries"]}, function (err, result) {
            if ( err ) throw err;
         });
         bucket.upsert('users_devUser2', user, function (err, result) {
            if ( err ) throw err;
         });

         var crudRead = daoActionTemplate;
         crudRead.dao_actions = actionGenReadId;

         try {
            helper.evaluateMessage(crudRead, mockcb(cb))
         } catch ( e ) {
            console.log("Should try get object: ", e)
         }
      })
   });

   describe('Test CRUD GENERIC_UPDATE',function() {

      var user = {username: 'users_devUser',password: 'users_devUser',scope: 'user', cloudlet: 'c_1bb237a4556baf0dd99d7d4a2f9f2df0'}


      var actionGenUpdateId = [
         {
            'action'       : 'GENERIC_UPDATE',
            'database'     : 'users',
            'id'           : 'users_devUser2',
            'data'         : user,
            'authorization': 'dbkeys_29f81fe0-3097-4e39-975f-50c4bf8698c7',
            'options'      : {}
         }
      ];


      it("Should try crud update", function (done) {
         var cb = function (err, results, httpStatusCode) {
            assert.isNull(err, "Error should be null");
            assert.isNotNull(results, "results should not be null");
            assert.equal(200,httpStatusCode, "Status should be 201");
            assert.equal(results['response']['username'], user['username'], "IDs should match")
            done()
         };

         bucket.upsert('dbkeys_29f81fe0-3097-4e39-975f-50c4bf8698c7', {"dbs": ["users","clients","authorizations","queries"]}, function (err, result) {
            if ( err ) throw err;
         });
         bucket.upsert('users_devUser2', user, function (err, result) {
            if ( err ) throw err;
         });

         var crudUpdate = daoActionTemplate;
         crudUpdate.dao_actions = actionGenUpdateId;

         try {
            helper.evaluateMessage(crudUpdate, mockcb(cb))
         } catch ( e ) {
            console.log("Should try get object: ", e)
         }
      })
   });

   describe('Test CRUD GENERIC_VIEW',function() {
      var actionGenViewId = [
         {
            'action'     : 'GENERIC_VIEW',
            'start_key'  : 'c_897b0ef002da79321dcb0d681cb473d0',
            'end_key'    : 'c_897b0ef002da79321dcb0d681cb473d0' + '\uefff',
            'design_doc' : 'clients_views',
            'view_name'  : "clients_by_cloudlet_id",
            'meta'       : {
               "limit"       : 30,
               "offset"      : 0,
               "total_count" : 0,
               "prev"        : null,
               "next"        : 30
            },
            'resp_type'  : 'clients',
            'cloudlet'   : 'c_897b0ef002da79321dcb0d681cb473d0',
            'bucket'     : 'clients',
            'data'       : {}
         }
      ];

      var client = {
         "name": "mochaTest",
         "description": "MochaTest",
         "isSE": false,
         "cloudlet": "c_897b0ef002da79321dcb0d681cb473d0",
         "api_key": "3c78fe66d77c5cbe2bdef13efe10e3c3",
         "secret": "cc2db5e5b3ad33587f89430c05d5ff94dc68952ac68750d11b7898b968630acf"
      };
      var clients_views = {
         "views": {
            "clients_by_cloudlet_id": { "map"   : "function (doc, meta) {\n  if (undefined !== doc.isTest && true === doc.isTest ){ return }\n emit(doc.cloudlet, doc); \n}", "reduce": "_count" },
            "list_service_enablers" : { "map"   : "function (doc, meta) {\n if (undefined !== doc.isTest && true === doc.isTest ){ return }\n if (undefined !== doc.isSE && true === doc.isSE ){\n emit(meta.id, doc);\n }\n}", "reduce": "_count" }
         }
      };
      var user = {username: 'users_devUser',password: 'users_devUser',scope: 'user', cloudlet: 'c_1bb237a4556baf0dd99d7d4a2f9f2df0'}


      it("Should try crud GENERIC_VIEW", function (done) {
         var cb = function (err, results, httpStatusCode) {
            assert.isNull(err, "Error should be null");
            assert.isNotNull(results, "results should not be null");
            assert.equal(200,httpStatusCode, "Status should be 201");
            //assert.equal(results['response']['username'], user['username'], "IDs should match")
            done()
         };

         bucket.upsert('clients_3c78fe66d77c5cbe2bdef13efe10e3c3', client, function (err, result) {
            if ( err ) throw err;
         });
         bucket.upsert('dbkeys_29f81fe0-3097-4e39-975f-50c4bf8698c7', {"dbs": ["users","clients","authorizations","queries"]}, function (err, result) {
            if ( err ) throw err;
         });

         bucketMngr.upsertDesignDocument("clients_views",clients_views,function (err, result) {
            assert.isNull(err, "Error should be null");
            if ( err ) throw err;
            bucket._indexView("clients_views","clients_by_cloudlet_id",null,function(err){
               assert.isNull(err, "Error should be null");
               if ( err ) throw err;

               var crudUpdate = daoActionTemplate;
               crudUpdate.dao_actions = actionGenViewId;

               try {
                  helper.evaluateMessage(crudUpdate, mockcb(cb))
               } catch ( e ) {
                  console.log("Should try get object: ", e)
               }
            })
         });
      })
   });

   describe('Test CRUD PATCH_ATTACHMENT_OBJECT',function() {
      var obj = {"@data":{'t_670a968a4a7bbe6fbbd434d23ed756c0-1667':'obj'}};

      var actionPatchAttach = [
         {
            'action'       : 'PATCH_ATTACHMENT_OBJECT',
            'database'     : 'c_897b0ef002da79321dcb0d681cb473d0+0c7d1319-684c-876d-20d5-f99df576376c',
            'type'         : 't_670a968a4a7bbe6fbbd434d23ed756c0-1667',
            'object'       : obj,
            'bucket'       : 'objects'
         }
      ];

      var clients_views = {
         "views": {
            "clients_by_cloudlet_id": { "map"   : "function (doc, meta) {\n  if (undefined !== doc.isTest && true === doc.isTest ){ return }\n emit(doc.cloudlet, doc); \n}", "reduce": "_count" },
            "list_service_enablers" : { "map"   : "function (doc, meta) {\n if (undefined !== doc.isTest && true === doc.isTest ){ return }\n if (undefined !== doc.isSE && true === doc.isSE ){\n emit(meta.id, doc);\n }\n}", "reduce": "_count" }
         }
      };
      var user = {username: 'users_devUser',password: 'users_devUser',scope: 'user', cloudlet: 'c_1bb237a4556baf0dd99d7d4a2f9f2df0'}


      it("Should try crud PATCH_ATTACHMENT_OBJECT", function (done) {
         var cb = function (err, results, httpStatusCode) {
            console.log(err, results, httpStatusCode);
            assert.isNull(err, "Error should be null");
            assert.isNotNull(results, "results should not be null");
            assert.equal(200,httpStatusCode, "Status should be 200");
            //assert.equal(results['response']['username'], user['username'], "IDs should match")
            done()
         };

         var data = {'t_670a968a4a7bbe6fbbd434d23ed756c0-1667':'obj'};
         exampleObject['@data'] = data

         bucket.upsert('dbkeys_29f81fe0-3097-4e39-975f-50c4bf8698c7', {"dbs": ["users","clients","authorizations","queries"]}, function (err, result) {
            if ( err ) throw err;
         });

         bucket.upsert('c_897b0ef002da79321dcb0d681cb473d0+0c7d1319-684c-876d-20d5-f99df576376c', exampleObject, function (err, result) {
            if ( err ) throw err;
         });

         var actionPatch = daoActionTemplate;
         actionPatch.dao_actions = actionPatchAttach;

         try {
            helper.evaluateMessage(actionPatch, mockcb(cb))
         } catch ( e ) {
            console.log("Should try PATCH_ATTACHMENT_OBJECT: ", e)
         }
      })
   });

   //describe('Test CRUD PATCH_ATTACHMENT',function() {
   //   var obj = {"@data":['a','b','c','d','e','f','g','h','i','j','k']};
   //
   //   var actionPatchAttach = [
   //      {
   //         'action'       : 'PATCH_ATTACHMENT',
   //         'database'     : 'c_897b0ef002da79321dcb0d681cb473d0+0c7d1319-684c-876d-20d5-f99df576376c',
   //         'type'         : 't_670a968a4a7bbe6fbbd434d23ed756c0-1667',
   //         'object'       : obj,
   //         'bucket'       : 'objects',
   //         'property'     : '1.2.3.4.5.6.7.8.9.0'
   //      }
   //   ];
   //
   //   var clients_views = {
   //      "views": {
   //         "clients_by_cloudlet_id": { "map"   : "function (doc, meta) {\n  if (undefined !== doc.isTest && true === doc.isTest ){ return }\n emit(doc.cloudlet, doc); \n}", "reduce": "_count" },
   //         "list_service_enablers" : { "map"   : "function (doc, meta) {\n if (undefined !== doc.isTest && true === doc.isTest ){ return }\n if (undefined !== doc.isSE && true === doc.isSE ){\n emit(meta.id, doc);\n }\n}", "reduce": "_count" }
   //      }
   //   };
   //   var user = {username: 'users_devUser',password: 'users_devUser',scope: 'user', cloudlet: 'c_1bb237a4556baf0dd99d7d4a2f9f2df0'}
   //
   //
   //   it("Should try crud PATCH_ATTACHMENT", function (done) {
   //      var cb = function (err, results, httpStatusCode) {
   //         console.log(err, results, httpStatusCode);
   //         assert.isNull(err, "Error should be null");
   //         assert.isNotNull(results, "results should not be null");
   //         assert.equal(200,httpStatusCode, "Status should be 200");
   //         //assert.equal(results['response']['username'], user['username'], "IDs should match")
   //         done()
   //      };
   //
   //      var data = ['a','b','c','d','e','f','g','h','i','j','k'];
   //      exampleObject['@data'] = data;
   //
   //      bucket.upsert('dbkeys_29f81fe0-3097-4e39-975f-50c4bf8698c7', {"dbs": ["users","clients","authorizations","queries"]}, function (err, result) {
   //         if ( err ) throw err;
   //      });
   //
   //      bucket.upsert('c_897b0ef002da79321dcb0d681cb473d0+0c7d1319-684c-876d-20d5-f99df576376c', exampleObject, function (err, result) {
   //         if ( err ) throw err;
   //      });
   //
   //      var actionPatch = daoActionTemplate;
   //      actionPatch.dao_actions = actionPatchAttach;
   //
   //      try {
   //         helper.evaluateMessage(actionPatch, mockcb(cb))
   //      } catch ( e ) {
   //         console.log("Should try PATCH_ATTACHMENT_OBJECT: ", e)
   //      }
   //   })
   //});

});
