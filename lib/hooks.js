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

/**
 * A map of transformer operation hooks.
 * Hooks are organized as <ns>.<stage>-<step>, where:
 * - 'ns' is a name that allows commonly named hooks to be
 *   grouped across multiple pipelines (i.e. if the same hook can
 *   be triggered by different pipelines). The namespace is specified
 *   when a pipeline is created;
 * - 'stage' is either 'pre' or 'post';
 * - 'name' is a hook name, specified when a step is defined.
 */
const Hooks = {};

/**
 * Register a transformer operation hook.
 * @param ns    The hook namespace.
 * @param stage The step stage, i.e. 'pre' or 'post'.
 * @param name  The operation name.
 * @param hook  The hook function.
 */
function registerHook( ns, stage, name, hook ) {
    let hookNS = Hooks[ns];
    if( !hookNS ) {
        hookNS = Hooks[ns] = {};
    }
    let id = `${stage}-${name}`;
    let hooks = hookNS[id];
    if( hooks ) {
        hooks.push( hook );
    }
    else {
        hooks = hookNS[id] = [ hook ];
    }
}

/**
 * Return a function for invoking transformer operation call hooks.
 * See the lineTransformer() function below.
 * @param ns        A hook namespace.
 * @param stage     The processing stage name, e.g. 'pre' or 'post'.
 * @param name      A hook name.
 */
function getCallHooks( ns, stage, name ) {
    // Lookup the hook namespace.
    let hooks = Hooks[ns];
    // Lookup hooks for the specified stage and hook name.
    hooks = hooks && hooks[`${stage}-${name}`];
    // If no hooks then return the identity function.
    if( !hooks ) {
        return value => value;
    }
    // If only one hook then return just that.
    if( hooks.length == 1 ) {
        return hooks[0];
    }
    // If multiple hooks then eturn a function to invoke all hooks 
    // in sequence.
    return async function( value, vars ) {
        for( let hook of hooks ) {
            value = await hook( value, vars );
        }
        return value;
    }
}

module.exports = { registerHook, getCallHooks };

