'use strict';

module.exports = function(grunt) {

   var lib         = 'lib/**/*.js';

   grunt.initConfig({
      pkg: grunt.file.readJSON('package.json'),
      jshint: {
         all: [lib],
         options: grunt.file.readJSON('.jshintrc')
      }
   });

   // These plugins provide necessary tasks.
   grunt.loadNpmTasks('grunt-contrib-jshint');

   // Default task(s).
   grunt.registerTask('default',  ['jshint' ]);

};
