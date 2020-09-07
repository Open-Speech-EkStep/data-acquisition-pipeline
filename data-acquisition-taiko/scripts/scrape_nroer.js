const fs = require('fs')
const { openBrowser, goto, text, $, click} = require('taiko');
(async () => {
    let i = 0;
    
    try {
        await openBrowser({headless: false});
        await goto('https://nroer.gov.in/home/e-library/filter?selfilters=[{%22or%22:[{%22selFieldValue%22:%22Language%22,%22selFieldValueAltnames%22:%22Language%22,%22selFieldGstudioType%22:%22field%22,%22selFieldText%22:%22Hindi%22,%22selFieldPrimaryType%22:%22basestring%22}]},{%22or%22:[{%22selFieldValue%22:%22educationallevel%22,%22selFieldValueAltnames%22:%22Level%22,%22selFieldGstudioType%22:%22attribute%22,%22selFieldText%22:%22Senior%20Secondary%22,%22selFieldPrimaryType%22:%22list%22}]}]', {timeout: 60000});

        let i = 0;
        while (true){
            
            await click(text("Audios"));
            i += 1;

            file_to_save = '../data/scraped/audios_sr_secondary/page_' + String(i) + '.txt'

            try{

                let data_text = (await $(".scard-title").elements()).map(e => { return e.text() })
                let file_titles = await Promise.all(data_text) 

                let scard_elements = await $('div > .scard').elements();
                let file_type_attribute_promises = scard_elements.map(e => {
                   return evaluate(e, elem => {if (elem.getAttribute('data-mime-type') !== null ){return elem.getAttribute('data-mime-type');}});
                });
                file_types = await Promise.all(file_type_attribute_promises) 

                let new_scard_elements = await $('div > .scard').elements();
                let files_links_promises = new_scard_elements.map(e => {
                   return evaluate(e, elem => {return elem.getAttribute('original-src');});
                });
                file_links = await Promise.all(files_links_promises) 

                // saving to file
                
                final_file_content = file_titles + '\n' + file_types + '\n' + file_links
                writeintoFile(final_file_content, file_to_save)
                
            }
            catch(e){
                console.log("Excpetion occurred")
            }
            
            await click(text(String(i+1)));
            await waitFor(10000)
    }
    }
   
    catch (error) {
        console.error(error);
    } finally {
        await closeBrowser();

    }
})();

writeintoFile = (values, file_name)=>{

    fs.writeFile(file_name, values, function (err) {
        if (err) throw err;
      }); 
}


