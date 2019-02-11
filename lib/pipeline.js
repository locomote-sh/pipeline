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

const Path = require('path');

const { StepResult } = require('./result');

/**
 * A pipeline processing step.
 */
class Step {

    /**
     * Construct a new step.
     * @param fn    The step function.
     * @param cachePath A pattern for generating the path to the step's
     *                  cache file. Can be null if the step's results
     *                  aren't cached.
     * @param prevStep  The previous step in the pipeline; can be null
     *                  if constructing the initial pipeline step.
     */
    constructor( fn, cachePath, prevStep ) {
        this._fn = fn;
        this._cachePath = cachePath;
        this._prevStep = prevStep;
    }

    /**
     * Return a readble stream on this step's result.
     */
    readable( vars ) {
        return this.invoke( vars ).readable();
    }

    /**
     * Invoke the pipeline step.
     * @param vars Pipeline invocation variables.
     * @return A step result.
     */
    invoke( vars ) {
        // Create an object to encapsulate the step result.
        let result = new StepResult( this._cachePath, vars, this._fn, this._prevStep );
        return result;
    }

}

/**
 * A multi-step data processing pipeline.
 */
class Pipeline {

    /**
     * Construct a new pipeline.
     * @param cacheDir  The path under which step cache files are stored.
     */
    constructor( cacheDir ) {
        this._cacheDir = cacheDir;
        // Default init function.
        this._init = () => {};
    }

    /**
     * Set the pipeline init function.
     * @param init  The pipeline initialization function; called once
     *              at the start of every pipeline invocation, and used
     *              to initialize invocation variables.
     */
    init( init ) {
        this._init = init;
        return this;
    }

    /**
     * Open the pipeline with the first processing step. A synonym for
     * Pipeline.step() - the first step in the pipeline doesn't have
     * a data input, so this function can be used to clearly indicate
     * the case.
     */
    open( fn, cachePath ) {
        return this.step( fn, cachePath );
    }

    /**
     * Add a processing step to the pipeline.
     * @param fn    The step processing function. Should be an asynchronous
     *              function accepting the following arguments:
     *              - vars: The invocation variables for the current pipeline
     *              invocation - the value returned by the pipeline's init()
     *              function.
     *              - outs: An writeable stream to write the step's output to.
     *              - ins: A readable stream with data from the preceeding step;
     *              will be null if the current step is the pipeline's first step.
     * @param cachePath A file path pattern which describes the location of
     *              the step's cache file. When the step function is invoked,
     *              the file path pattern is resolved against the invocation
     *              variables and the step's result is written to the file. Next
     *              time the pipeline is invoked with the same arguments, the
     *              result is read from the cache file rather than recalculated.
     *              This can be particularly beneficial for multi-step pipelines,
     *              when the result only has to be read from the latest cached
     *              step result.
     *              If no cache path pattern is specified then the step's results
     *              are not cached.
     * @return A promise which is resolved once the step has completed
     * processing.
     */
    step( fn, cachePath ) {
        if( cachePath ) {
            cachePath = Path.join( this._cacheDir, cachePath );
        }
        this._lastStep = new Step( fn, cachePath, this._lastStep );
        return this;
    }

    /**
     * Indicate that the pipeline is complete. This method returns a function
     * which can be used to invoke the pipeline.
     * @param fn    A function to modify or annotate the step result returned
     *              by the pipeline; passed [ vars, result ] arguments.
     */
    done( fn = ( vars, result ) => result ) {
        // Check that the pipeline is setup correctly.
        if( !this._lastStep ) {
            throw new Error('Pipeline must have at least one processing step');
        }
        const pipeline = this;
        /**
         * A function for invoking the pipeline.
         * The function arguments are defined by the pipeline's init() function.
         * The function returns a readable stream on the pipeline's output, or
         * false if the pipeline won't generate any content for the request.
         */
        return async function() {
            // Generate the invocation vars by calling the applying the init
            // function to this function's arguments.
            const vars = await pipeline._init.apply( pipeline, arguments );
            // If init function return false then it indicates that there is
            // no content associated with the request - return false here to
            // signal the condition to the pipeline client.
            if( vars === false ) {
                return false;
            }
            // Invoke the last pipeline step - this will delegate to preceeding
            // pipeline steps as necessary.
            let result = pipeline._lastStep.invoke( vars );
            // Modify the pipeline result before returning.
            return fn( vars, result );
        }
    }

}

module.exports = { Step, Pipeline }

