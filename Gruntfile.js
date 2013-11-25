var grunt = require('grunt');
module.exports = function () {
    grunt.initConfig({
      traceur: {
          options: {
            sourceMaps: true // default: false
          },
          custom: {
            files:{
              'build/': ['src/**/*.js'] // dest : [source files]
            }
          },
        }
    });
    grunt.loadNpmTasks('grunt-traceur');
    grunt.registerTask('default', ['traceur'])
};
