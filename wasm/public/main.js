import init, { run_query, list_table } from "./pkg/bogosql_wasm.js";

init().then(() => {
    const tables = list_table();
    for (let table of tables) {
        const examples = document.getElementById("examples");
        const link = document.createElement("a");
        link.href = "#";
        link.innerHTML = table;
        link.addEventListener("click", () => {
            const query = document.getElementById("query");
            query.value = `SELECT * FROM ${table}`;
        })
        examples.appendChild(link);
        examples.appendChild(document.createTextNode(" "));
    }
});

function onQuery() {
    const query = document.getElementById("query");
    const queryValue = query.value;
    try {
        const res = run_query(queryValue);
        const resultElem = document.getElementById("result");
        resultElem.value = res;
    }
    catch(e) {
        const resultElem = document.getElementById("result");
        resultElem.value = `ERROR: ${e}`;
    }
}

const button = document.getElementById("runQuery");
button.addEventListener("click", onQuery);
