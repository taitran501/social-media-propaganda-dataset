// This JavaScript code is designed to be executed in the developer console of a web browser to scrape comments from a Threads post.

(async function() {
    console.log("Starting Threads comment scraper...");
    
    // More reliable than querySelector for some complex selections
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
        return String(s).replaceAll('"', '""');
    }
    
    function formatDateTime(isoString) {
        if (!isoString) return "";
        try {
            const date = new Date(isoString);
            if (isNaN(date.getTime())) return isoString;
            
            const day = String(date.getDate()).padStart(2, '0');
            const month = String(date.getMonth() + 1).padStart(2, '0');
            const year = date.getFullYear();
            
            return `${day}-${month}-${year}`;
        } catch (e) {
            console.error("Error formatting date:", e);
            return isoString;
        }
    }
    
    // Extract post ID from URL or page elements
    function extractPostId() {
        try {
            // Try to extract post ID from URL
            const currentUrl = window.location.href;
            const postIdMatch = currentUrl.match(/\/post\/([A-Za-z0-9_-]+)/);
            if (postIdMatch && postIdMatch[1]) {
                return postIdMatch[1];
            }
            
            // Try to find it in links on page
            const postLinks = getElementsByXPath('//a[contains(@href, "/post/")]');
            if (postLinks && postLinks.length > 0) {
                for (const link of postLinks) {
                    if (link.href) {
                        const match = link.href.match(/\/post\/([A-Za-z0-9_-]+)/);
                        if (match && match[1]) {
                            return match[1];
                        }
                    }
                }
            }
            
            // Fallback ID using timestamp
            return `thread_${Date.now()}`;
        } catch (e) {
            console.error("Error extracting post ID:", e);
            return `thread_${Date.now()}`;
        }
    }
    
    // Thêm hàm MD5 hash để tạo comment_id
    function md5(input) {
        // Một triển khai MD5 thuần JavaScript
        function add32(a, b) {
            return (a + b) & 0xFFFFFFFF;
        }

        function cmn(q, a, b, x, s, t) {
            a = add32(add32(a, q), add32(x, t));
            return add32((a << s) | (a >>> (32 - s)), b);
        }

        function ff(a, b, c, d, x, s, t) {
            return cmn((b & c) | ((~b) & d), a, b, x, s, t);
        }

        function gg(a, b, c, d, x, s, t) {
            return cmn((b & d) | (c & (~d)), a, b, x, s, t);
        }

        function hh(a, b, c, d, x, s, t) {
            return cmn(b ^ c ^ d, a, b, x, s, t);
        }

        function ii(a, b, c, d, x, s, t) {
            return cmn(c ^ (b | (~d)), a, b, x, s, t);
        }

        function md5cycle(x, k) {
            let a = x[0], b = x[1], c = x[2], d = x[3];

            a = ff(a, b, c, d, k[0], 7, -680876936);
            d = ff(d, a, b, c, k[1], 12, -389564586);
            c = ff(c, d, a, b, k[2], 17, 606105819);
            b = ff(b, c, d, a, k[3], 22, -1044525330);
            a = ff(a, b, c, d, k[4], 7, -176418897);
            d = ff(d, a, b, c, k[5], 12, 1200080426);
            c = ff(c, d, a, b, k[6], 17, -1473231341);
            b = ff(b, c, d, a, k[7], 22, -45705983);
            a = ff(a, b, c, d, k[8], 7, 1770035416);
            d = ff(d, a, b, c, k[9], 12, -1958414417);
            c = ff(c, d, a, b, k[10], 17, -42063);
            b = ff(b, c, d, a, k[11], 22, -1990404162);
            a = ff(a, b, c, d, k[12], 7, 1804603682);
            d = ff(d, a, b, c, k[13], 12, -40341101);
            c = ff(c, d, a, b, k[14], 17, -1502002290);
            b = ff(b, c, d, a, k[15], 22, 1236535329);

            a = gg(a, b, c, d, k[1], 5, -165796510);
            d = gg(d, a, b, c, k[6], 9, -1069501632);
            c = gg(c, d, a, b, k[11], 14, 643717713);
            b = gg(b, c, d, a, k[0], 20, -373897302);
            a = gg(a, b, c, d, k[5], 5, -701558691);
            d = gg(d, a, b, c, k[10], 9, 38016083);
            c = gg(c, d, a, b, k[15], 14, -660478335);
            b = gg(b, c, d, a, k[4], 20, -405537848);
            a = gg(a, b, c, d, k[9], 5, 568446438);
            d = gg(d, a, b, c, k[14], 9, -1019803690);
            c = gg(c, d, a, b, k[3], 14, -187363961);
            b = gg(b, c, d, a, k[8], 20, 1163531501);
            a = gg(a, b, c, d, k[13], 5, -1444681467);
            d = gg(d, a, b, c, k[2], 9, -51403784);
            c = gg(c, d, a, b, k[7], 14, 1735328473);
            b = gg(b, c, d, a, k[12], 20, -1926607734);

            a = hh(a, b, c, d, k[5], 4, -378558);
            d = hh(d, a, b, c, k[8], 11, -2022574463);
            c = hh(c, d, a, b, k[11], 16, 1839030562);
            b = hh(b, c, d, a, k[14], 23, -35309556);
            a = hh(a, b, c, d, k[1], 4, -1530992060);
            d = hh(d, a, b, c, k[4], 11, 1272893353);
            c = hh(c, d, a, b, k[7], 16, -155497632);
            b = hh(b, c, d, a, k[10], 23, -1094730640);
            a = hh(a, b, c, d, k[13], 4, 681279174);
            d = hh(d, a, b, c, k[0], 11, -358537222);
            c = hh(c, d, a, b, k[3], 16, -722521979);
            b = hh(b, c, d, a, k[6], 23, 76029189);
            a = hh(a, b, c, d, k[9], 4, -640364487);
            d = hh(d, a, b, c, k[12], 11, -421815835);
            c = hh(c, d, a, b, k[15], 16, 530742520);
            b = hh(b, c, d, a, k[2], 23, -995338651);

            a = ii(a, b, c, d, k[0], 6, -198630844);
            d = ii(d, a, b, c, k[7], 10, 1126891415);
            c = ii(c, d, a, b, k[14], 15, -1416354905);
            b = ii(b, c, d, a, k[5], 21, -57434055);
            a = ii(a, b, c, d, k[12], 6, 1700485571);
            d = ii(d, a, b, c, k[3], 10, -1894986606);
            c = ii(c, d, a, b, k[10], 15, -1051523);
            b = ii(b, c, d, a, k[1], 21, -2054922799);
            a = ii(a, b, c, d, k[8], 6, 1873313359);
            d = ii(d, a, b, c, k[15], 10, -30611744);
            c = ii(c, d, a, b, k[6], 15, -1560198380);
            b = ii(b, c, d, a, k[13], 21, 1309151649);
            a = ii(a, b, c, d, k[4], 6, -145523070);
            d = ii(d, a, b, c, k[11], 10, -1120210379);
            c = ii(c, d, a, b, k[2], 15, 718787259);
            b = ii(b, c, d, a, k[9], 21, -343485551);

            x[0] = add32(a, x[0]);
            x[1] = add32(b, x[1]);
            x[2] = add32(c, x[2]);
            x[3] = add32(d, x[3]);
        }

        function md5blk(s) {
            const md5blks = [];
            for (let i = 0; i < 64; i += 4) {
                md5blks[i >> 2] = s.charCodeAt(i) + (s.charCodeAt(i + 1) << 8) + (s.charCodeAt(i + 2) << 16) + (s.charCodeAt(i + 3) << 24);
            }
            return md5blks;
        }

        function md5blk_array(a) {
            const md5blks = [];
            for (let i = 0; i < 64; i += 4) {
                md5blks[i >> 2] = a[i] + (a[i + 1] << 8) + (a[i + 2] << 16) + (a[i + 3] << 24);
            }
            return md5blks;
        }

        function md51(s) {
            const n = s.length;
            const state = [1732584193, -271733879, -1732584194, 271733878];
            let i;
            
            for (i = 64; i <= s.length; i += 64) {
                md5cycle(state, md5blk(s.substring(i - 64, i)));
            }
            
            s = s.substring(i - 64);
            const tail = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
            
            for (i = 0; i < s.length; i++) {
                tail[i >> 2] |= s.charCodeAt(i) << ((i % 4) << 3);
            }
            
            tail[i >> 2] |= 0x80 << ((i % 4) << 3);
            
            if (i > 55) {
                md5cycle(state, tail);
                for (i = 0; i < 16; i++) tail[i] = 0;
            }
            
            tail[14] = n * 8;
            md5cycle(state, tail);
            return state;
        }

        function hex_md5(s) {
            const result = md51(s);
            let output = '';
            for (let i = 0; i < 4; i++) {
                for (let j = 0; j < 32; j += 8) {
                    output += ((result[i] >>> j) & 0xff).toString(16).padStart(2, '0');
                }
            }
            return output;
        }

        return hex_md5(input);
    }

    // Thay thế hàm createCommentId hiện tại với phiên bản sử dụng MD5
    function createCommentId(author, commentText, timestamp) {
        // Kết hợp author + comment + timestamp để tạo input cho MD5
        // Đảm bảo tính nhất quán bằng cách chuẩn hóa các input
        const normalizedAuthor = (author || "unknown").trim();
        const normalizedComment = (commentText || "").trim().substring(0, 100); // Lấy 100 ký tự đầu để tránh comment quá dài
        const normalizedTimestamp = (timestamp || "").trim();
        
        // Tạo chuỗi input cho MD5 hash
        const inputString = `${normalizedAuthor}|${normalizedComment}|${normalizedTimestamp}|threads_comment`;
        
        // Tạo hash MD5
        const md5Hash = md5(inputString);
        
        // Thêm prefix "tc_" (threads_comment) để dễ nhận biết
        return `tc_${md5Hash}`;
    }
    
// Better post content extraction using flexible XPath selectors
function extractPostContent() {
    try {
        // Define container selectors that commonly hold post content
        const mainContentContainers = [
            '//div[contains(@class, "x1a6qonq")]',
            '//div[@role="main"]//div[contains(@class, "x1a6qonq")]',
            '//div[contains(@class, "x6s0dn4")]//div[contains(@class, "x1a6qonq")]'
        ];
        
        // First approach: Look for the exact structure shown in the example
        for (const containerXPath of mainContentContainers) {
            const containers = getElementsByXPath(containerXPath);
            if (containers && containers.length > 0) {
                for (const container of containers) {
                    // Find all spans with the specific class used for post content
                    const contentSpans = container.querySelectorAll('span[class*="x1lliihq"][dir="auto"]');
                    
                    if (contentSpans && contentSpans.length > 0) {
                        // Filter out very short spans and combine the rest
                        const textContent = Array.from(contentSpans)
                            .map(span => span.innerText.trim())
                            .filter(text => text.length > 5) // Filter out very short segments
                            .join('\n\n');
                        
                        if (textContent && textContent.length > 10) {
                            console.log("Found post content:", textContent.substring(0, 50) + "...");
                            return textContent;
                        }
                    }
                }
            }
        }
        
        // Second approach: Try alternative selectors for post content
        const postContentXPaths = [
            // Spans with specific classes common in post content
            '//span[@dir="auto" and contains(@class, "x1lliihq")]',
            // Post content in block containers
            '//div[contains(@class, "x1a6qonq")]//span[@dir="auto"]',
            // Secondary approach using tabindex
            '//span[@dir="auto" and @tabindex="0"]',
        ];
        
        for (const xpath of postContentXPaths) {
            const elements = getElementsByXPath(xpath);
            if (elements && elements.length > 0) {
                // Combine all likely post content spans
                const textContent = elements
                    .map(el => el.innerText.trim())
                    .filter(text => text.length > 10 && !text.startsWith('@') && !text.match(/^\d+\s+(giờ|phút)/))
                    .join('\n\n');
                
                if (textContent && textContent.length > 15) {
                    console.log("Found post content (approach 2):", textContent.substring(0, 50) + "...");
                    return textContent;
                }
            }
        }
        
        // Fallback to looking for any substantial text blocks near the top of the page
        const allSpans = document.querySelectorAll('span[dir="auto"]');
        const possibleContent = Array.from(allSpans)
            .filter(span => {
                const text = span.innerText.trim();
                // Look for longer text blocks that aren't navigation elements
                return text.length > 25 && 
                       !text.startsWith('@') && 
                       !text.match(/^\d+\s+(giờ|phút)/);
            })
            .map(span => span.innerText.trim())
            .join('\n\n');
        
        if (possibleContent.length > 0) {
            console.log("Found post content (fallback):", possibleContent.substring(0, 50) + "...");
            return possibleContent;
        }
        
        return "";
    } catch (e) {
        console.error("Error extracting post content:", e);
        return "";
    }
}
    
    // Extract post author and metadata with resilient selectors
    function extractPostMetadata() {
        const metadata = {
            postAuthor: "Unknown",
            postTime: "Unknown",
            postUrl: window.location.href
        };
        
        try {
            // Extract author directly from URL
            const currentUrl = window.location.href;
            
            // Try to extract author from URL path pattern (threads.com/username/post/id)
            const urlAuthorMatch = currentUrl.match(/threads\.net\/([^\/]+)\/post/i) || 
                                  currentUrl.match(/threads\.com\/([^\/]+)\/post/i) || 
                                  currentUrl.match(/\/([^\/]+)\/post/);
                                  
            if (urlAuthorMatch && urlAuthorMatch[1]) {
                // Get the username without @ symbol
                metadata.postAuthor = urlAuthorMatch[1];
            } else {
                // Fallback to other approaches if URL extraction fails
                const authorXPaths = [
                    '//a[@role="link" and contains(@href, "/@")]',
                    '//div[contains(@class, "x1pi30zi")]//a',
                    '//span[contains(@class, "x1lliihq") and contains(text(), "@")]'
                ];
                
                for (const xpath of authorXPaths) {
                    const elements = getElementsByXPath(xpath);
                    if (elements && elements.length > 0) {
                        for (const el of elements) {
                            // Try to extract from href first (most reliable)
                            if (el.href) {
                                const match = el.href.match(/\/([^\/]+)(?:\/post|\?)/);
                                if (match && match[1]) {
                                    metadata.postAuthor = match[1]; // Without @ symbol
                                    break;
                                }
                            }
                            
                            // Otherwise try text content
                            const text = el.innerText.trim();
                            if (text.includes('@')) {
                                // Remove the @ symbol
                                metadata.postAuthor = text.replace('@', '');
                                break;
                            }
                        }
                        
                        if (metadata.postAuthor !== "Unknown") break;
                    }
                }
            }
            
            // Post time - always in a time element
            const timeXPaths = [
                '//time[@datetime]',
                '//a//time[@datetime]'
            ];
            
            for (const xpath of timeXPaths) {
                const elements = getElementsByXPath(xpath);
                if (elements && elements.length > 0) {
                    const datetime = elements[0].getAttribute('datetime');
                    if (datetime) {
                        metadata.postTime = formatDateTime(datetime);
                        break;
                    }
                }
            }
            
            // Post URL - try to find the canonical URL
            const urlXPaths = [
                '//a[contains(@href, "/t/")]',
                '//link[@rel="canonical"]/@href'
            ];
            
            for (const xpath of urlXPaths) {
                const elements = getElementsByXPath(xpath);
                if (elements && elements.length > 0) {
                    const href = elements[0].href || elements[0].value;
                    if (href && href.includes('/t/')) {
                        metadata.postUrl = href;
                        break;
                    }
                }
            }
        } catch (e) {
            console.error("Error extracting post metadata:", e);
        }
        
        return metadata;
    }

    // Extract post content, ID and metadata
    const postContent = extractPostContent();
    const postId = extractPostId();
    const { postAuthor, postTime, postUrl } = extractPostMetadata();
    
    // Collect comments
    const comments = [];
    let loadingBuffer = 3; // Buffer for loading attempts
    
    // Find comment elements using more resilient selectors
    function findCommentElements() {
        // Try multiple XPath expressions to find comments with fallbacks
        const commentXPaths = [
            // Comments by role
            '//div[@role="article"]',
            '//article[@role="article"]',
            // Comments by structure (under main content)
            '//section//div[contains(@role, "button")]//following-sibling::div',
            // Comments by class pattern
            '//div[contains(@class, "x1y332i5")]',
            '//div[contains(@class, "xz9dl7a")]',
            // Comments by time presence (replies usually have timestamps)
            '//div[.//time]'
        ];
        
        // Try each XPath expression
        for (const xpath of commentXPaths) {
            const elements = getElementsByXPath(xpath);
            if (elements.length > 0) {
                // Filter elements to get only comments (exclude post content, etc.)
                const commentElements = elements.filter(el => {
                    // Comments typically:
                    // 1. Are not too short
                    // 2. Often contain usernames, timestamps, or comment text
                    const text = el.innerText.trim();
                    const hasTime = el.querySelector('time') !== null;
                    const hasLink = el.querySelector('a[role="link"]') !== null;
                    
                    return text.length > 10 && (hasTime || hasLink);
                });
                
                if (commentElements.length > 0) {
                    console.log(`Found ${commentElements.length} comments with XPath: ${xpath}`);
                    return commentElements;
                }
            }
        }
        
        // Fallback approach using structural analysis
        console.log("No comments found with standard selectors, trying structural analysis...");
        
        const possibleComments = [];
        const mainContent = document.querySelector('div[role="main"], main');
        
        if (mainContent) {
            // Look for div elements under the main content that might be comments
            const allDivs = mainContent.querySelectorAll('div');
            
            for (const div of allDivs) {
                // Skip tiny elements or large containers
                if (!div.innerText || div.innerText.length < 10 || div.children.length > 15) continue;
                
                // Comments typically have:
                // 1. A username
                // 2. Time element
                // 3. Are not too deep in the DOM
                const hasUsername = div.innerText.match(/\@[a-zA-Z0-9._]+/) !== null;
                const hasTime = div.querySelector('time') !== null;
                const hasLink = div.querySelector('a[role="link"]') !== null;
                
                if ((hasUsername || hasTime || hasLink) && div.innerText.length < 1000) {
                    possibleComments.push(div);
                }
            }
        }
        
        console.log(`Found ${possibleComments.length} possible comments using structural analysis`);
        return possibleComments;
    }
    
    // Improved comment text cleaning with more robust pattern handling
    function cleanCommentText(text) {
        // Check if text is too short to clean
        if (!text || text.length < 1) return text;
        
        // Save original text for fallback
        const originalText = text;
        
        // First approach: Extract actual comment content after time/author markers
        let cleanedText = text;
        
        // Try to match time/author patterns at the beginning
        const beginningPatterns = [
            // Time ago at the beginning, like "4 giờ"
            /^\d+\s+(giờ|phút|ngày|tuần|tháng|năm)(?:\s*\n|\s*·|\s*$)/i,
            
            // Date patterns at the beginning, like "21/04/2025"
            /^(\d{1,2}\/\d{1,2}\/\d{2,4})(?:\s*\n|\s*·|\s*$)/i,
            
            // Author markers like "· Tác giả" or just "Tác giả"
            /(?:^|\n)\s*·\s*\n?\s*Tác giả\s*\n/i,
            /(?:^|\n)\s*Tác giả\s*\n/i,
            
            // Pinned comment marker
            /^Đã ghim\s*\n/i,
        ];
        
        // Function to extract content after a specific marker
        function extractContentAfterMarker(text, marker) {
            const match = text.match(marker);
            if (match && match.index !== undefined) {
                // Get content after the marker
                return text.substring(match.index + match[0].length).trim();
            }
            return null;
        }
        
        // Try to extract content after common markers
        for (const pattern of beginningPatterns) {
            const content = extractContentAfterMarker(cleanedText, pattern);
            if (content) {
                cleanedText = content;
            }
        }
        
        // More comprehensive pattern (time + author + content)
        if (cleanedText === text) {
            const fullPattern = /(?:\d+\s+(?:giờ|phút|ngày|tuần|tháng|năm)|(?:\d{1,2}\/\d{1,2}\/\d{2,4}))\s*\n?\s*(?:·\s*\n?\s*Tác giả\s*\n)?([\s\S]*)/i;
            const match = text.match(fullPattern);
            if (match && match[1]) {
                cleanedText = match[1].trim();
            }
        }
        
        // ENHANCED CLEANING: More aggressively handle metrics and reaction counts
        
        // Remove trailing social media metrics with K suffix (1K, 1.5K, 1,9K format)
        cleanedText = cleanedText.replace(/\s*[\d.,]+K\s*$/gi, '');
        
        // Remove international number formats with spaces (1, 9K or 1.9 K)
        cleanedText = cleanedText.replace(/\s*\d+\s*[,.:]\s*\d+\s*K\s*$/gi, '');
        
        // Remove standalone numbers at the end that might be reaction counts
        cleanedText = cleanedText.replace(/\s+\d+\s*$/g, '');
        
        // More aggressive cleaning for complex number patterns
        cleanedText = cleanedText.replace(/\s*\d+(?:\s*[,.\s]\s*\d+)*\s*$/g, '');
        
        // Remove any lines that consist only of numbers, metrics or dates
        let lines = cleanedText.split('\n');
        const filteredLines = lines.filter(line => {
            const trimmedLine = line.trim();
            // Skip empty lines, number-only lines, date-only lines, or metric-only lines
            return trimmedLine !== '' && 
                   !/^\s*\d+\s*$/.test(trimmedLine) &&
                   !/^\s*\d{1,2}\/\d{1,2}\/\d{4}\s*$/.test(trimmedLine) &&
                   !/^\s*[\d,.]+K?\s*$/.test(trimmedLine);
        });
        cleanedText = filteredLines.join('\n');
        
        // Handle very short comments or single characters
        if (cleanedText.length <= 2 && /^[.,:;]$/.test(cleanedText.trim())) {
            // If it's just a single punctuation mark, consider it non-meaningful
            return "";
        }
        
        // Final cleanup of extra whitespace and newlines
        cleanedText = cleanedText.trim()
                               .replace(/^\n+/, '')
                               .replace(/\n\n+/g, '\n\n')
                               .trim();
        
        // If we somehow ended up with an empty string but original was substantial,
        // return original with basic cleaning
        if (cleanedText.length < 3 && originalText.length > 10) {
            // Try a last basic cleanup on the original
            return originalText.trim().replace(/\s*[\d,.]+K\s*$/g, '');
        }
        
        return cleanedText;
    }
    
    // Enhanced username extraction
    function extractUsername(commentElement) {
        // Try different approaches to extract username
        
        // Approach 1: Find an anchor with a username pattern
        const userLinks = Array.from(commentElement.querySelectorAll('a[role="link"], a[href*="/@"]'));
        for (const link of userLinks) {
            // Check if the link text looks like a username
            const text = link.innerText.trim();
            if (text && !text.includes(' ') && text.length < 30) {
                return text;
            }
            
            // Extract from href if available
            if (link.href) {
                const match = link.href.match(/\/@([^/?]+)/);
                if (match && match[1]) {
                    return match[1];
                }
            }
        }
        
        // Approach 2: Look for text patterns that might be usernames
        const text = commentElement.innerText;
        const usernameMatch = text.match(/\@[a-zA-Z0-9._]+/);
        if (usernameMatch) {
            return usernameMatch[0];
        }
        
        // Approach 3: First short text block might be a username
        const textNodes = Array.from(commentElement.querySelectorAll('span, div'))
            .filter(el => el.innerText.trim().length > 0)
            .map(el => el.innerText.trim());
        
        if (textNodes.length > 0) {
            const firstText = textNodes[0];
            if (firstText.length < 30 && !firstText.includes('\n')) {
                return firstText;
            }
        }
        
        return "Unknown";
    }
    
    // Extract comment data from elements
    function extractCommentData() {
        const commentElements = findCommentElements();
        let newCommentsFound = 0;
        
        for (const el of commentElements) {
            // Extract username with enhanced approach
            const username = extractUsername(el);
            
            // Get raw comment text
            let commentText = el.innerText.trim();
            
            // If username was found, remove it from comment text
            if (username !== "Unknown") {
                commentText = commentText.replace(username, '').trim();
            }
            
            // Extract timestamp using time element and format it to dd-mm-yyyy
            let timestamp = "";
            const timeElement = el.querySelector('time');
            if (timeElement && timeElement.hasAttribute('datetime')) {
                // Apply formatDateTime to convert to dd-mm-yyyy format
                timestamp = formatDateTime(timeElement.getAttribute('datetime'));
            }
            
            // Clean up the comment text
            commentText = cleanCommentText(commentText);
            
            // Generate a unique comment ID based on author and content
            const commentId = createCommentId(username, commentText, timestamp);
            
            // Skip if we've already processed a very similar comment (same author + content)
            const isDuplicate = comments.some(c => 
                c.author === username && 
                c.comment_raw === commentText && 
                c.created_date === timestamp
            );
            
            if (isDuplicate) continue;
            
            // Add to comments array if there's actual content
            if (commentText && commentText.length > 0) {
                comments.push({
                    comment_id: commentId,
                    author: username,
                    comment_raw: commentText,
                    created_date: timestamp
                });
                newCommentsFound++;
            }
        }
        
        return newCommentsFound;
    }
    
    // Scroll to bottom of comments to load more
    async function scrollToLoadMoreComments() {
        // Try to find comment section or just scroll the page
        const commentSection = document.querySelector('section, div[role="main"]');
        
        // If we found a specific section, scroll it
        if (commentSection) {
            commentSection.scrollTop = commentSection.scrollHeight;
        } else {
            // Otherwise scroll the window
            window.scrollTo(0, document.body.scrollHeight);
        }
        
        console.log("Scrolled to load more comments");
        
        // Wait for new comments to load
        await new Promise(resolve => setTimeout(resolve, 1350));
        return true;
    }
    
    // Main scraping loop
    console.log("Starting to scrape comments...");
    let previousCommentCount = 0;
    
    while (loadingBuffer > 0) {
        // Extract currently visible comments
        const newCommentsFound = extractCommentData();
        
        // Log progress
        console.log(`Found ${comments.length} total comments (${newCommentsFound} new)`);
        
        // Check if we found new comments
        if (comments.length > previousCommentCount) {
            previousCommentCount = comments.length;
            loadingBuffer = 3; // Reset buffer when we make progress
            
            // Scroll to try to load more
            await scrollToLoadMoreComments();
        } else {
            // No new comments found, decrease buffer
            loadingBuffer--;
            console.log(`No new comments found. ${loadingBuffer} attempts remaining`);
            
            // Still try to scroll in case it helps
            await scrollToLoadMoreComments();
        }
    }
    
    console.log(`Finished scraping. Total comments: ${comments.length}`);

    // Function to properly escape CSV values
    function escapeCsvValue(value) {
        if (value === null || value === undefined) return '';
        return '"' + String(value).replace(/"/g, '""') + '"';
    }

    // Generate CSV content
    function generateCsv(comments, postId, postContent) {
        // CSV header
        const header = ["post_id", "post_raw", "comment_id", "author", "created_date", "comment_raw"];
        let csvContent = header.join(",") + "\n";
        
        // CSV rows
        for (const comment of comments) {
            const row = [
                escapeCsvValue(postId),
                escapeCsvValue(postContent),
                escapeCsvValue(comment.comment_id),
                escapeCsvValue(comment.author),
                escapeCsvValue(comment.created_date),
                escapeCsvValue(comment.comment_raw)
            ];
            csvContent += row.join(",") + "\n";
        }
        
        return csvContent;
    }

    // Generate the CSV
    const csvContent = generateCsv(comments, postId, postContent);

    // Output CSV to console with clear markers
    console.log("\n\n=== CSV DATA BEGIN ===");
    console.log(csvContent);
    console.log("=== CSV DATA END ===");

    console.log("\nCopy everything between CSV DATA BEGIN and END markers");
    console.log("Then run 'Extract Comments from Clipboard.cmd'");

})();