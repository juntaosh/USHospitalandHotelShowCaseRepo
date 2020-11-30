function autocompletandcorrect(input) {
    input.addEventListener("input", function (e) {
        var parent_node, query = this.value;
        closeAutocomplete();
        if(!query) {
            return false;
        }
        else {
            parent_node = document.createElement("DIV");
            parent_node.setAttribute("id", this.id + "autocomplete-list");
            parent_node.setAttribute("class", "autocomplete-options");
            this.parentNode.appendChild(parent_node);
            $.ajax({
                type: "GET",
                url: '/autocomplete',
                dataType: "text",
                data: "query=" + encodeURI(input.value),
                contentType: 'application/json;charset=UTF-8',
                success: function (data) {
                    console.log(data);
                    addAutocomplete(JSON.parse(data).history, parent_node, "history")
                    addAutocomplete(JSON.parse(data).autocorrect, parent_node, "autocorrect")
                }
            });
        }
    })

    function addAutocomplete(data_array, parent_node, mode) {
        for (var i = 0; i < data_array.length; i++) {
            var child_node = document.createElement("DIV");
            if (mode == "history") {
                child_node.innerHTML = "<strong>" + data_array[i] +"</strong>";
            }
            else {
                var all_words = data_array[i].split(" ");
                var end_string = all_words[all_words.length - 1];
                var end_string_length = end_string.length;
                var original_length = data_array[i].length;
                var start_string = data_array[i].substr(0, original_length - end_string_length)
                child_node.innerHTML = "<strong>" + start_string +"</strong>";
                child_node.innerHTML += end_string
            }
            child_node.innerHTML += "<input type='hidden' value='" + data_array[i] + "'>";
            child_node.addEventListener("click", function (e) {
                input.value = this.getElementsByTagName("input")[0].value;
                closeAutocomplete();
            });
            parent_node.appendChild(child_node);
        }
    }

    function closeAutocomplete(option) {
        var autocompletes = document.getElementsByClassName("autocomplete-options");
        for(var i = 0; i < autocompletes.length; ++i) {
            if(option != autocompletes[i] && option != input) {
                autocompletes[i].parentNode.removeChild(autocompletes[i])
            }
        }
    }
};
autocompletandcorrect(document.getElementById("query"));