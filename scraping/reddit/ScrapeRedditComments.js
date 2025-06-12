with({
    copy
}) {
    // XPath selectors for Reddit
    var allCommentsXPath = '//shreddit-comment';
    var postIdXPath = '//shreddit-post'; 
    var postTitleXPath = '//h1[@id[starts-with(., "post-title")]]';
    var postContentXPath = '//div[starts-with(@id, "t3_") and contains(@id, "-post-rtjson-content")]/p';
    // Comprehensive selector for all types of expand buttons
    var expandButtonXPath = '//button[contains(@class, "text-tone-2") and (.//svg[@icon-name="join-outline"] or contains(., "more repl") or contains(., "Continue this thread") or contains(., "Show more comments") or contains(., "view all"))]|//button[contains(text(), "View") and contains(text(), "more")]|//button[contains(@aria-label, "more")]|//button[contains(@aria-label, "expand")]';
    
    // Helper functions
    function getElementsByXPath(xpath, parent) {
        let results = [];
        let query = document.evaluate(xpath, parent || document,
            null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null);
        for (let i = 0, length = query.snapshotLength; i < length; ++i) {
            results.push(query.snapshotItem(i));
        }
        return results;
    }

    function quoteString(s) {
        return '"' + String(s).replaceAll('"', '""') + '"';
    }

    function formatDate(strDate) {
        if (typeof strDate !== 'undefined' && strDate !== null) {
            // If already in a date format with hyphens
            if (strDate.includes('-')) {
                f = strDate.split('-');
                if (f.length == 1) {
                    return strDate;
                } else if (f.length == 2) {
                    return f[1] + '-' + f[0] + '-' + (new Date().getFullYear());
                } else if (f.length == 3) {
                    return f[2] + '-' + f[1] + '-' + f[0];
                }
            }
            
            // Handle relative time (X ago format)
            const now = new Date();
            // Adjust to GMT+7
            const offsetHours = 7 - (-now.getTimezoneOffset() / 60);
            now.setHours(now.getHours() + offsetHours);
            
            let targetDate = new Date(now);
            
            if (strDate.toLowerCase().includes('ago')) {
                const value = parseInt(strDate);
                
                if (strDate.includes('s')) {
                    // Seconds ago
                    targetDate.setSeconds(targetDate.getSeconds() - value);
                } else if (strDate.includes('m') && !strDate.includes('mo')) {
                    // Minutes ago
                    targetDate.setMinutes(targetDate.getMinutes() - value);
                } else if (strDate.includes('h')) {
                    // Hours ago
                    targetDate.setHours(targetDate.getHours() - value);
                } else if (strDate.includes('d')) {
                    // Days ago
                    targetDate.setDate(targetDate.getDate() - value);
                } else if (strDate.includes('w')) {
                    // Weeks ago
                    targetDate.setDate(targetDate.getDate() - (value * 7));
                } else if (strDate.includes('mo')) {
                    // Months ago
                    targetDate.setMonth(targetDate.getMonth() - value);
                } else if (strDate.includes('y')) {
                    // Years ago
                    targetDate.setFullYear(targetDate.getFullYear() - value);
                }
                
                // Format as dd-mm-yyyy
                const day = String(targetDate.getDate()).padStart(2, '0');
                const month = String(targetDate.getMonth() + 1).padStart(2, '0');
                const year = targetDate.getFullYear();
                
                return `${day}-${month}-${year}`;
            }
            
            return strDate;
        } else {
            return 'No date';
        }
    }
    
    // Track which buttons we've already clicked
    const clickedButtons = new Set();
    
    // Click expand/join buttons to show all comments
    async function clickExpandButtons(maxDepth = 20, currentDepth = 0) {
        // Prevent infinite recursion
        if (currentDepth >= maxDepth) {
            console.log(`Reached maximum recursion depth (${maxDepth})`);
            return;
        }
        
        // Find all expand buttons
        const expandButtons = getElementsByXPath(expandButtonXPath);
        let foundNewButtons = false;
        
        if (expandButtons && expandButtons.length > 0) {
            console.log(`Found ${expandButtons.length} expand buttons to click (depth: ${currentDepth})`);
            
            // Group buttons by their vertical position for more efficient processing
            const buttonGroups = {};
            expandButtons.forEach((button, index) => {
                if (!clickedButtons.has(button)) {
                    const rect = button.getBoundingClientRect();
                    const yPos = Math.floor(rect.top / 500); // Group by 500px vertical segments
                    buttonGroups[yPos] = buttonGroups[yPos] || [];
                    buttonGroups[yPos].push(button);
                }
            });
            
            // Process each group of buttons
            for (const yPos in buttonGroups) {
                const buttons = buttonGroups[yPos];
                console.log(`Processing ${buttons.length} buttons in group ${yPos}`);
                
                // First, scroll to this group
                if (buttons.length > 0) {
                    buttons[0].scrollIntoView({ behavior: 'auto', block: 'center' });
                    // Brief pause to ensure page stabilizes after scroll
                    await new Promise(r => setTimeout(r, 300));
                }
                
                // Click all buttons in this group
                for (let button of buttons) {
                    if (clickedButtons.has(button)) continue;
                    
                    try {
                        console.log('Clicking expand button: ' + (button.textContent?.trim() || 'unknown'));
                        button.click();
                        clickedButtons.add(button);
                        foundNewButtons = true;
                        
                        // Give a brief wait between clicks in same area (don't need long waits)
                        await new Promise(r => setTimeout(r, 300));
                    } catch (e) {
                        console.log(`Error clicking expand button: ${e}`);
                    }
                }
                
                // After processing a group, wait a bit longer to ensure all content loads
                await new Promise(r => setTimeout(r, 500));
            }
            
            // If we found and clicked new buttons, do another pass
            if (foundNewButtons) {
                // Small wait to let content load before next recursive call
                await new Promise(r => setTimeout(r, 1000));
                
                // Process any newly revealed buttons
                await clickExpandButtons(maxDepth, currentDepth + 1);
            }
        } else {
            console.log('No expand buttons found at depth ' + currentDepth);
        }
    }
    
    // Function to process deeply nested comments
    async function processNestedComments(maxIterations = 5) {
        for (let iteration = 0; iteration < maxIterations; iteration++) {
            console.log(`Deep comment expansion iteration ${iteration + 1}/${maxIterations}...`);
            
            // Try to find any remaining buttons
            const remainingButtons = getElementsByXPath(expandButtonXPath);
            const unclickedButtons = remainingButtons.filter(button => !clickedButtons.has(button));
            
            if (unclickedButtons.length === 0) {
                console.log('No more expand buttons found');
                break;
            }
            
            console.log(`Found ${unclickedButtons.length} more buttons to click`);
            
            // Click all remaining buttons
            for (let button of unclickedButtons) {
                try {
                    button.scrollIntoView({ behavior: 'auto', block: 'center' });
                    await new Promise(r => setTimeout(r, 300));
                    
                    if (!clickedButtons.has(button)) {
                        console.log('Clicking button: ' + (button.textContent?.trim() || 'unknown'));
                        button.click();
                        clickedButtons.add(button);
                        
                        // Brief wait between clicks
                        await new Promise(r => setTimeout(r, 300));
                    }
                } catch (e) {
                    console.log(`Error in deep expansion: ${e}`);
                }
            }
            
            // Wait for content to load before next iteration
            await new Promise(r => setTimeout(r, 1500));
            
            // Scroll up and down to trigger any lazy-loading
            window.scrollTo(0, 0);
            await new Promise(r => setTimeout(r, 500));
            window.scrollTo(0, document.body.scrollHeight);
            await new Promise(r => setTimeout(r, 500));
        }
        
        console.log('Finished deep comment expansion');
    }

    // Load all comments by scrolling and expanding
    async function loadAllComments() {
        console.log('Starting comment expansion...');
        
        // First, scroll through the page to trigger lazy loading
        let previousHeight = 0;
        let currentHeight = document.body.scrollHeight;
        let attempts = 0;
        
        while (previousHeight !== currentHeight && attempts < 15) {
            previousHeight = currentHeight;
            
            // Scroll to the bottom
            window.scrollTo(0, document.body.scrollHeight);
            
            // Wait for content to load
            await new Promise(r => setTimeout(r, 800));
            
            // Get new height
            currentHeight = document.body.scrollHeight;
            attempts++;
            
            console.log(`Initial scrolling... (${attempts}/15)`);
        }
        
        // Now click expand buttons throughout the page
        await clickExpandButtons();
        
        // Scroll through again to ensure all content is loaded
        previousHeight = 0;
        currentHeight = document.body.scrollHeight;
        attempts = 0;
        
        while (previousHeight !== currentHeight && attempts < 5) {
            previousHeight = currentHeight;
            
            // Scroll to the bottom
            window.scrollTo(0, document.body.scrollHeight);
            
            // Wait for content to load
            await new Promise(r => setTimeout(r, 800));
            
            // Get new height
            currentHeight = document.body.scrollHeight;
            attempts++;
            
            console.log(`Final scrolling... (${attempts}/5)`);
        }
        
        // Do a deep recursive pass to catch nested comment buttons
        await processNestedComments();
        
        console.log('Finished loading all comments!');
    }

    // Helper function to extract specific comment content with proper ID
    function extractCommentContent(comment) {
        try {
            // Get the comment's ID from its attributes
            const commentId = comment.getAttribute('thingid');
            if (!commentId) return null;
            
            // Use the ID to find exactly this comment's content
            const exactContentId = `${commentId}-post-rtjson-content`;
            
            // Look specifically for the div that has this exact ID
            const contentDiv = comment.querySelector(`div[id="${exactContentId}"]`);
            if (!contentDiv) return null;
            
            // Only use direct paragraph children from this exact div
            const paragraphs = contentDiv.querySelectorAll('p');
            if (!paragraphs || paragraphs.length === 0) return null;
            
            // Combine the paragraphs
            return Array.from(paragraphs)
                .map(p => p.textContent.trim())
                .filter(text => text.length > 0)
                .join(' ');
        } catch (e) {
            console.log(`Error in extractCommentContent: ${e}`);
            return null;
        }
    }

    // Main scraping function
    async function scrapeRedditComments() {
        // Load all comments first
        await loadAllComments();
        
        // Get post details
        let post_id;
        let post_raw = "";
        
        // Get post ID from URL or post element
        const url = window.location.href.split('?')[0];
        if (url.includes('/comments/')) {
            post_id = url.split('/comments/')[1].split('/')[0];
        } else {
            const postElem = getElementsByXPath(postIdXPath)[0];
            if (postElem && postElem.getAttribute('id')) {
                post_id = postElem.getAttribute('id');
            } else {
                // Generate a random ID if not found
                post_id = "post_" + Math.floor(Math.random() * 1000000);
            }
        }
        
        // Get post title and content to combine into post_raw
        try {
            // Get post title
            let post_title = "";
            const titleElems = getElementsByXPath(postTitleXPath);
            if (titleElems && titleElems.length > 0) {
                post_title = titleElems[0].textContent.trim();
            }
            
            // Get post content
            let post_content = "";
            const contentElems = getElementsByXPath(postContentXPath);
            if (contentElems && contentElems.length > 0) {
                post_content = Array.from(contentElems)
                    .map(el => el.textContent.trim())
                    .join(' ');
            }
            
            // Combine title and content with a line break between them
            if (post_title && post_content) {
                post_raw = post_title + "\n" + post_content;
            } else if (post_title) {
                post_raw = post_title;
            } else if (post_content) {
                post_raw = post_content;
            } else {
                post_raw = "N/A";
            }
            
        } catch (e) {
            console.log(`Error getting post raw: ${e}`);
            post_raw = "N/A";
        }
        
        // CSV header
        var csv = "post_id,post_raw,comment_id,author,created_date,comment_raw\n";
        
        // Get all comments
        const comments = getElementsByXPath(allCommentsXPath);
        console.log(`Found ${comments.length} comments`);
        
        // Simple comment counter starting from 1
        let commentCounter = 1;
        
        // Process each comment
        for (let i = 0; i < comments.length; i++) {
            const comment = comments[i];
            
            // Skip completely empty or severely damaged comments
            if (!comment.textContent || comment.textContent.trim().length === 0) {
                continue;
            }
            
            // Get comment ID - format as post_id_commentNumber
            let comment_id = post_id + "_" + commentCounter++;
            
            // Get author - checking for [deleted] in the text
            let author = comment.getAttribute('author') || "";
            if (!author || author.trim() === "") {
                // Check if the text contains [deleted]
                if (comment.textContent.includes("[deleted]")) {
                    author = "deleted_" + Math.floor(Math.random() * 10000);
                } else {
                    author = "Anonymous";
                }
            }
            
            // Get created date
            let created_date = "N/A";
            try {
                const timeElem = comment.querySelector('faceplate-timeago time');
                if (timeElem) {
                    created_date = formatDate(timeElem.textContent.trim());
                }
            } catch (e) {
                console.log(`Error getting date: ${e}`);
            }
            
            // Extract comment text using more precise method
            let comment_raw = extractCommentContent(comment);
            
            // Skip if we couldn't extract anything meaningful
            if (!comment_raw || comment_raw.trim().length === 0) {
                continue;
            }
            
            // Add to CSV
            csv += `${quoteString(post_id)},${quoteString(post_raw)},${quoteString(comment_id)},${quoteString(author)},${quoteString(created_date)},${quoteString(comment_raw)}\n`;
        }
        
        console.log(`Processed ${comments.length} comments`);
        
        // Copy to clipboard
        copy(csv);
        
        console.log('Data copied to clipboard! You can now paste it into the Python script.');
        
        return csv;
    }
    
    // Run the scraper
    scrapeRedditComments();
} 