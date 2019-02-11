/* 
   Copyright 2019 Locomote Ltd.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

const {
    exists,
    ensureDirForFile
} = require('@locomote.sh/utils');

const TT        = require('@locomote.sh/tinytemper');
const FS        = require('fs');
const Path      = require('path');
const Through   = require('through');

// Supported MIME types for cache files.
const MIMETypes = {
    '.json':    'application/json',
    '.jsonl':   'application/x-jsonlines',
    '.zip':     'application/zip'
}
const DefaultMIMEType = 'application/octet-stream';

/**
 * A pipeline step result. Encapsulates information about the step on-disk
 * cache file, and provides methods for reading the result content and writing
 * that content to a HTTP response.
 */
class StepResult {

    /**
     * Construct a new step result.
     * @param path  An optional cache file path. Can be specified as a text
     *              template, in which case vars need to be supplied.
     * @param vars  Variables for resolving the cache file path template.
     */
    constructor( path, vars, fn, input ) {
        if( path ) {
            this._path = vars ? TT.eval( path, vars ) : path;
            this.ext = Path.extname( this._path );
            this.mimeType = MIMETypes[this.ext] || DefaultMIMEType;
        }
        this._vars  = vars;
        this._fn    = fn;
        this._input = input;
    }

    toJSON() {
        return this._path;
    }

    /**
     * Test whether a cached result exists on disk.
     */
    _cached() {
        if( !this._path ) {
            return false;
        }
        return exists( this._path );
    }

    /**
     * Prepare the step result to start receiving data from the
     * step function. If the step result is cached to disk then
     * creates a split read/write stream which both writes to the
     * cache file and forwards the data through its readable stream
     * interface. If the step result isn't cached to disk then
     * simply forwards the data.
     */
    async _output() {
        let outs;
        if( this._path ) {
            await ensureDirForFile( this._path );
            // Step is cached so open a writeable stream on the cache
            // file.
            const fouts = FS.createWriteStream( this._path );
            // Create a readable + writeable stream which will accept
            // data through its writeable interface, write that data
            // to the cache file, and forward the same data so that it
            // is available through its readable interface.
            outs = Through(
                function write( data ) {
                    fouts.write( data );
                    this.queue( data );
                },
                function end() {
                    fouts.close();
                    this.queue( null );
                });
        }
        else {
            // No on-disk file, so just copy input through.
            outs = Through(
                function write( data ) {
                    this.queue( data );
                },
                function end() {
                    this.queue( null );
                });
        }
        return outs;
    }

    /**
     * Return a readable stream on the step result's data. If a
     * read/write stream has been created using the prepare() method
     * then that is returned; otherwise if the result is cached then
     * a readable stream on the cache file is opened; otherwise no
     * stream is returned.
     */
    async readable() {
        // Check for a previously cached result.
        let cached = await this._cached();
        if( cached ) {
            return FS.createReadStream( this._path );
        }
        // Open a readable stream on the input, if any.
        let ins;
        if( this._input ) {
            ins = await this._input.readable( this._vars );
            // NOTE pause the input stream until the pipeline
            // is fully setup, to avoid data being written
            // before all steps are in this place.
            ins.pause();
        }
        // Open the output stream.
        let outs = await this._output();
        // Invoke the step function.
        this._fn( this._vars, outs, ins );
        // If there is an input stream then unpause it now that the
        // pipeline is configured.
        if( ins ) {
            ins.resume();
        }
        // The output stream is also readable, return as the result.
        return outs;
    }

    /**
     * Pipe the result's data to an output stream.
     */
    async pipe( outs ) {
        const ins = await this.readable();
        if( !ins ) {
            throw new Error('Unable to open readable on cache result');
        }
        ins.pipe( outs );
    }

    /**
     * Send the step result to a HTTP response.
     */
    async send( res, headers ) {
        // Open a readable stream on the result data.
        const ins = await this.readable();
        // If unable to open a stream - which shouldn't happen - then
        // indicate an empty response.
        if( !ins ) {
            return res.sendStatus( 204 );
        }
        // Flag to indicate whether data has been written to the client.
        let writeStarted = false;
        // Receive result data.
        ins.on('data', data => {
            if( data.length > 0 ) {
                try {
                    // If this is the first chunk then write response headers.
                    if( !writeStarted ) {
                        res.set( headers );
                        writeStarted = true;
                    }
                    res.write( data );
                }
                catch( e ) {} // Swallow any errors when writing data to client.
            }
        });
        // End of result data.
        ins.on('end', () => {
            // If no data written then signal an empty response.
            if( !writeStarted ) {
                res.sendStatus( 204 );
            }
            res.end();
        });
        // Error reading data.
        ins.on('error', err => {
            Log.error('Sending filedb result', err );
            if( writeStarted ) {
                // If write has already started and an error occurs then
                // it's too late to send an error status code, so all we
                // can do is prematurely terminate the response.
                return res.end();
            }
            // TODO Need to confirm that missing file errors will appear
            // like this.
            if( err.code == 'ENOENT' ) {
                res.sendStatus( 404 );
            }
            // Send a 500 status code.
            res.sendStatus( 500 );
        });
    }
}

StepResult.fromJSON = function( json ) {
    // JSON indicates the cache file path - see StepResult.toJSON().
    let result = new StepResult( json );
}

module.exports = { StepResult };

