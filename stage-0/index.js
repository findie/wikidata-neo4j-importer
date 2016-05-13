'use strict';
module.exports = (neo4j, callback) => {
    const session = neo4j.session();

    console.log('Clearing DB');

    session
        .run('MATCH (n) DETACH DELETE n')
        .subscribe({
            onCompleted: ()=> {
                session.close();
                return callback();
            },
            onError: (e)=> {
                session.close();
                callback(e);
            }
        })
}