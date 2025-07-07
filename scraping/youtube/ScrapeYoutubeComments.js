with({
    copy
}) {
    // XPath selectors được cập nhật dựa trên HTML
    const commentBlockXPath = './/ytd-comment-thread-renderer';
    const readMoreXPath = './/tp-yt-paper-button[@id="more" and not(@hidden)]';
    
    // XPath cho button replies - cập nhật dựa trên HTML thực tế
    const showRepliesXPath = './/ytd-comment-replies-renderer//tp-yt-paper-button[contains(@aria-label, "phản hồi")]';
    
    // XPath cho button "Hiện thêm phản hồi"
    const showMoreRepliesXPath = './/tp-yt-paper-button[contains(@aria-label, "Hiện thêm phản hồi")]';

    // XPath cho "Show more comments"
    const showMoreCommentsXPath = './/ytd-continuation-item-renderer//tp-yt-paper-button';

    function getElementsByXPath(xpath, parent) {
        let results = [];
        let query = document.evaluate(xpath, parent || document, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null);
        for (let i = 0, length = query.snapshotLength; i < length; ++i) {
            results.push(query.snapshotItem(i));
        }
        return results;
    }

    function quoteString(s) {
        return '"' + String(s).replaceAll('"', '""') + '"';
    }

    function scrollToCommentsSection() {
        let commentsSection = document.querySelector('ytd-comments#comments');
        if (commentsSection) {
            commentsSection.scrollIntoView({ behavior: 'smooth' });
        }
    }

    function isClickable(element) {
        if (!element || !element.offsetParent) return false;
        if (element.disabled || element.hasAttribute('disabled')) return false;
        if (element.hasAttribute('hidden') || element.style.display === 'none') return false;
        if (element.offsetWidth === 0 || element.offsetHeight === 0) return false;
        
        const styles = getComputedStyle(element);
        if (styles.visibility === 'hidden' || styles.opacity === '0') return false;
        
        return true;
    }

    // Hàm xử lý và làm sạch comment text
    function processCommentText(text) {
        if (!text || typeof text !== 'string') return null;
        
        // Loại bỏ emoji Unicode - fix regex syntax
        text = text.replace(/[\uD83C-\uDBFF][\uDC00-\uDFFF]|[\u2600-\u27FF]|[\uD83C][\uDF00-\uDFFF]/g, '');
        
        // Loại bỏ emoticon text như :)), =)), :D, :P, etc.
        text = text.replace(/[:=xX][\-\^]?[\)\(\[\]DdPpOo3<>\/\\|}{@*]+/g, '');
        text = text.replace(/[\)\(\[\]DdPpOo3<>\/\\|}{@*]+[:=xX][\-\^]?/g, '');
        
        // Loại bỏ các chuỗi ký tự lặp lại (kéo dài) như "hahahahaha", "wowwwww", "niceeeee"
        text = text.replace(/(.)\1{2,}/g, '$1$1'); // Giữ lại tối đa 2 ký tự liên tiếp
        
        // Chỉ loại bỏ các ký tự thực sự không mong muốn, giữ lại dấu / và dấu câu cơ bản
        text = text.replace(/[^\w\s\u00C0-\u024F\u1E00-\u1EFF\u0100-\u017F.,!?;:()\-\/'"]/g, ' ');
        
        // Loại bỏ khoảng trắng thừa
        text = text.replace(/\s+/g, ' ').trim();
        
        return text;
    }

    function isValidComment(text) {
        if (!text) return false;
        
        // Xử lý text trước
        const processedText = processCommentText(text);
        if (!processedText) return false;
        
        // Loại bỏ comment toàn emoji - fix regex syntax
        const emojiOnlyRegex = /^[\uD83C-\uDBFF\uDC00-\uDFFF\u2600-\u27FF\s]*$/;
        if (emojiOnlyRegex.test(text)) {
            console.log('Filtered: emoji-only comment');
            return false;
        }
        
        // Loại bỏ comment toàn emoticon text
        const emoticonOnlyRegex = /^[:=xX\-\^]*[\)\(\[\]DdPpOo3<>\/\\|}{@*\s]*$/;
        if (emoticonOnlyRegex.test(text)) {
            console.log('Filtered: emoticon-only comment');
            return false;
        }
        
        // Danh sách các từ viết tắt được phép (có thể mở rộng)
        const allowedAcronyms = [
            'vnch', 'vncs', 'vn'
        ];
        
        // Đếm số từ thực sự (không tính số và ký tự đặc biệt)
        const words = processedText.split(/\s+/).filter(word => 
            word.length > 0 && /[a-zA-Z\u00C0-\u024F\u1E00-\u1EFF\u0100-\u017F]/.test(word)
        );
        
        // Kiểm tra xem có từ viết tắt được phép không
        const hasAllowedAcronym = words.some(word => 
            allowedAcronyms.includes(word.toLowerCase())
        );
        
        // Nếu có từ viết tắt được phép, cho phép comment ngắn hơn
        const minWords = hasAllowedAcronym ? 1 : 3;
        
        if (words.length <= minWords) {
            if (hasAllowedAcronym) {
                console.log(`Allowed: contains acronym in "${processedText}"`);
                return true;
            } else {
                console.log(`Filtered: too short (${words.length} words): "${processedText}"`);
                return false;
            }
        }
        
        return true;
    }
    // DEBUG function để test selectors - CẬP NHẬT
    function debugButtons() {
        console.log('=== DEBUG BUTTONS ===');
        
        // Test tất cả button có aria-label chứa "phản hồi"
        let allReplyBtns = document.querySelectorAll('button[aria-label*="phản hồi"]');
        console.log(`All reply buttons found: ${allReplyBtns.length}`);
        
        // Lọc chỉ button show replies (có pattern số + phản hồi)
        let showReplyBtns = Array.from(allReplyBtns).filter(btn => {
            let ariaLabel = btn.getAttribute('aria-label');
            return /^\d+\s+phản\s+hồi$/.test(ariaLabel);
        });
        console.log(`Show reply buttons (X phản hồi): ${showReplyBtns.length}`);
        
        showReplyBtns.forEach((btn, i) => {
            if (i < 5) {
                console.log(`Show reply btn ${i}: "${btn.getAttribute('aria-label')}" - Clickable: ${isClickable(btn)} - Visible: ${btn.offsetParent !== null}`);
                // Check nếu đã có replies loaded
                let thread = btn.closest('ytd-comment-thread-renderer');
                let hasReplies = thread && thread.querySelector('ytd-comment-replies-renderer ytd-comment-view-model');
                console.log(`  Has loaded replies: ${!!hasReplies}`);
            }
        });
        
        // Test read more buttons
        let readMoreBtns = document.querySelectorAll('tp-yt-paper-button#more:not([hidden])');
        console.log(`Read more buttons found: ${readMoreBtns.length}`);
        
        console.log('=== END DEBUG ===');
    }

    // PHASE 1: Load all comments first - FIX SCROLL ĐẾN CUỐI
    async function loadAllComments() {
        console.log('PHASE 1: Loading all comments...');
        let lastCommentCount = 0;
        let stableCount = 0;
        let maxStableCount = 3;
        let iteration = 0;
        
        while (stableCount < maxStableCount && iteration < 50) {
            iteration++;
            console.log(`Load comments iteration ${iteration}`);
            
            let currentCommentCount = getElementsByXPath(commentBlockXPath).length;
            console.log(`Current comments: ${currentCommentCount}`);
            
            // Tìm button "Show more comments" - nhiều selector khác nhau
            let showMoreBtns = document.querySelectorAll([
                'ytd-continuation-item-renderer tp-yt-paper-button',
                'ytd-continuation-item-renderer button',
                'button[aria-label*="Hiển thị thêm"]',
                '#continuations button'
            ].join(', '));
            
            console.log(`Found ${showMoreBtns.length} potential show more buttons`);
            
            let clicked = 0;
            for (let btn of showMoreBtns) {
                let btnText = btn.textContent.trim().toLowerCase();
                let ariaLabel = btn.getAttribute('aria-label') || '';
                
                if (isClickable(btn) && (
                    btnText.includes('hiển thị') || 
                    btnText.includes('show more') ||
                    ariaLabel.includes('thêm bình luận')
                )) {
                    console.log(`Clicking show more: "${btnText}" - "${ariaLabel}"`);
                    btn.scrollIntoView({ behavior: 'smooth', block: 'center' });
                    await new Promise(r => setTimeout(r, 500));
                    btn.click();
                    clicked++;
                    await new Promise(r => setTimeout(r, 1500)); // Đợi comments load
                }
            }
            
            // Scroll down để trigger lazy loading - SCROLL ĐẾN CUỐI THAY VÌ FIXED
            if (clicked === 0) {
                // Scroll đến cuối trang để trigger lazy loading
                let lastComment = document.querySelector('ytd-comment-thread-renderer:last-child');
                if (lastComment) {
                    lastComment.scrollIntoView({ behavior: 'smooth', block: 'end' });
                    await new Promise(r => setTimeout(r, 1000));
                } else {
                    // Fallback: scroll xuống cuối trang
                    window.scrollTo(0, document.body.scrollHeight);
                    await new Promise(r => setTimeout(r, 1000));
                }
            }
            
            let newCommentCount = getElementsByXPath(commentBlockXPath).length;
            
            if (newCommentCount === currentCommentCount) {
                stableCount++;
                console.log(`No new comments. Stable count: ${stableCount}/${maxStableCount}`);
            } else {
                stableCount = 0;
                console.log(`New comments loaded: ${newCommentCount - currentCommentCount}`);
            }
            
            lastCommentCount = newCommentCount;
            
            if (iteration > 30 && clicked === 0) {
                console.log('No show more buttons found for too long. Breaking...');
                break;
            }
        }
        
        console.log(`PHASE 1 completed. Total comments: ${lastCommentCount}`);
    }

    // PHASE 2: Expand all content - PHƯƠNG PHÁP SCROLL HẾT XUỐNG CUỐI
async function expandAllContent() {
        console.log('PHASE 2: Expanding all content...');
        
        // Scroll xuống cuối để đảm bảo tất cả comments được load
        console.log('Scrolling to bottom to ensure all content is loaded...');
        let lastHeight = 0;
        let currentHeight = document.body.scrollHeight;
        
        while (lastHeight !== currentHeight) {
            lastHeight = currentHeight;
            window.scrollTo(0, document.body.scrollHeight);
            await new Promise(r => setTimeout(r, 2000)); // Tăng delay scroll
            currentHeight = document.body.scrollHeight;
        }
        
        console.log('Finished scrolling to bottom, now expanding content...');
        
        // Bây giờ scroll về đầu và click tất cả buttons
        window.scrollTo(0, 0);
        await new Promise(r => setTimeout(r, 2000)); // Tăng delay
        
        let iteration = 0;
        let maxIterations = 10;
        
        while (iteration < maxIterations) {
            iteration++;
            console.log(`Expand content iteration ${iteration}`);
            
            let totalClicked = 0;
            
            // Click tất cả read more buttons - CHẬM HƠN
            let readMoreBtns = document.querySelectorAll('tp-yt-paper-button#more:not([hidden])');
            console.log(`Found ${readMoreBtns.length} read more buttons`);
            for (let btn of readMoreBtns) {
                if (isClickable(btn)) {
                    btn.scrollIntoView({ behavior: 'smooth', block: 'center' }); // Smooth scroll
                    await new Promise(r => setTimeout(r, 500)); // Delay lâu hơn
                    btn.click();
                    totalClicked++;
                    await new Promise(r => setTimeout(r, 300)); // Delay sau click
                }
            }
            
            // Click tất cả reply buttons - CHỈ CLICK NẾU CHƯA ĐƯỢC EXPAND - CHẬM HƠN
            let allReplyBtns = document.querySelectorAll('button[aria-label*="phản hồi"]');
            let showReplyBtns = Array.from(allReplyBtns).filter(btn => {
                let ariaLabel = btn.getAttribute('aria-label');
                return /^\d+\s+phản\s+hồi$/.test(ariaLabel);
            });
            
            console.log(`Found ${showReplyBtns.length} show reply buttons`);
            
            for (let btn of showReplyBtns) {
                if (isClickable(btn)) {
                    // Kiểm tra xem replies đã được expand hay chưa
                    let thread = btn.closest('ytd-comment-thread-renderer');
                    let repliesContainer = thread ? thread.querySelector('ytd-comment-replies-renderer') : null;
                    let hasExpandedReplies = repliesContainer && repliesContainer.querySelector('ytd-comment-view-model');
                    
                    if (!hasExpandedReplies) {
                        console.log(`Clicking: "${btn.getAttribute('aria-label')}"`);
                        btn.scrollIntoView({ behavior: 'smooth', block: 'center' }); // Smooth scroll
                        await new Promise(r => setTimeout(r, 600)); // Delay lâu hơn
                        btn.click();
                        totalClicked++;
                        await new Promise(r => setTimeout(r, 800)); // Delay sau click lâu hơn
                    } else {
                        console.log(`Skipping already expanded: "${btn.getAttribute('aria-label')}"`);
                    }
                }
            }
            
            // Click tất cả more replies buttons - CHẬM HƠN
            let moreRepliesBtns = document.querySelectorAll('button[aria-label*="Hiện thêm phản hồi"]');
            console.log(`Found ${moreRepliesBtns.length} show more replies buttons`);
            
            for (let btn of moreRepliesBtns) {
                if (isClickable(btn)) {
                    btn.scrollIntoView({ behavior: 'smooth', block: 'center' }); // Smooth scroll
                    await new Promise(r => setTimeout(r, 400)); // Delay
                    btn.click();
                    totalClicked++;
                    await new Promise(r => setTimeout(r, 500)); // Delay sau click
                }
            }
            
            console.log(`Iteration ${iteration}: clicked ${totalClicked} buttons`);
            
            if (totalClicked === 0) {
                console.log('No more buttons to click. Breaking...');
                break;
            }
            
            await new Promise(r => setTimeout(r, 2000)); // Delay giữa các iteration lâu hơn
        }
        
        console.log('PHASE 2 completed.');
    }

    function getVideoId() {
        let url = window.location.href;
        let match = url.match(/[?&]v=([^&]+)/);
        return match ? match[1] : 'unknown';
    }

    function getVideoTitle() {
        let titleElement = document.querySelector('h1.ytd-watch-metadata yt-formatted-string') || 
                          document.querySelector('h1.title yt-formatted-string') || 
                          document.querySelector('h1.ytd-video-primary-info-renderer') ||
                          document.querySelector('h1.style-scope.ytd-video-primary-info-renderer') ||
                          document.querySelector('meta[property="og:title"]');
        
        if (titleElement) {
            return titleElement.content || titleElement.innerText.trim();
        }
        return 'N/A';
    }

    function getAuthorName(commentElement) {
        let authorElement = commentElement.querySelector('#author-text a') || 
                           commentElement.querySelector('#author-text span') ||
                           commentElement.querySelector('span[id="author-text"]') ||
                           commentElement.querySelector('#author-text');
        
        if (authorElement) {
            return authorElement.innerText.trim();
        }
        
        return 'Unknown';
    }

    function getCommentText(commentElement) {
        let contentElement = commentElement.querySelector('#content-text') ||
                            commentElement.querySelector('yt-attributed-string[id="content-text"]');
        
        if (contentElement) {
            let textSpan = contentElement.querySelector('span.yt-core-attributed-string');
            if (textSpan) {
                let clonedSpan = textSpan.cloneNode(true);
                // Remove emoji and mention links
                let mentionLinks = clonedSpan.querySelectorAll('a.yt-core-attributed-string__link');
                mentionLinks.forEach(link => link.remove());
                
                // Remove emoji images but keep emoji text
                let emojiImages = clonedSpan.querySelectorAll('img.yt-core-attributed-string__image-element');
                emojiImages.forEach(img => {
                    if (img.alt) {
                        img.replaceWith(img.alt);
                    } else {
                        img.remove();
                    }
                });
                
                return clonedSpan.innerText.trim();
            }
            
            return contentElement.innerText.trim();
        }
        
        return '';
    }

    function containsURL(text) {
        const urlRegex = /(https?:\/\/[^\s]+)|(www\.[^\s]+)|([^\s]+\.(com|org|net|edu|gov|io|co|tv|me|ly|be|gl)[^\s]*)/i;
        return urlRegex.test(text);
    }

    function getTimestamp(commentElement) {
        let timestampElement = commentElement.querySelector('#published-time-text a') ||
                              commentElement.querySelector('a[href*="lc="]') ||
                              commentElement.querySelector('#published-time-text');
        
        if (timestampElement) {
            return timestampElement.innerText.trim();
        }
        
        return 'N/A';
    }

    function convertYouTubeTimeToDate(ytTimeRaw) {
        const date = new Date();
        
        if (/^\d+\s+ngày\s+trước$/.test(ytTimeRaw)) {
            const days = parseInt(ytTimeRaw.match(/\d+/)[0]);
            date.setDate(date.getDate() - days);
        } else if (/^\d+\s+tuần\s+trước$/.test(ytTimeRaw)) {
            const weeks = parseInt(ytTimeRaw.match(/\d+/)[0]);
            date.setDate(date.getDate() - weeks * 7);
        } else if (/^\d+\s+tháng\s+trước$/.test(ytTimeRaw)) {
            const months = parseInt(ytTimeRaw.match(/\d+/)[0]);
            date.setMonth(date.getMonth() - months);
        } else if (/^\d+\s+năm\s+trước$/.test(ytTimeRaw)) {
            const years = parseInt(ytTimeRaw.match(/\d+/)[0]);
            date.setFullYear(date.getFullYear() - years);
        } else if (/^\d+\s+giờ\s+trước$/.test(ytTimeRaw)) {
            const hours = parseInt(ytTimeRaw.match(/\d+/)[0]);
            date.setHours(date.getHours() - hours);
        } else if (/^\d+\s+phút\s+trước$/.test(ytTimeRaw)) {
            const minutes = parseInt(ytTimeRaw.match(/\d+/)[0]);
            date.setMinutes(date.getMinutes() - minutes);
        } else {
            return ytTimeRaw;
        }

        const dd = String(date.getDate()).padStart(2, '0');
        const mm = String(date.getMonth() + 1).padStart(2, '0');
        const yyyy = date.getFullYear();
        return `${dd}-${mm}-${yyyy}`;
    }

    // PHASE 3: Collect all data với filter và clean - CẬP NHẬT
    async function collectData() {
        console.log('PHASE 3: Collecting data...');
        
        let video_id = getVideoId();
        let video_title = getVideoTitle();
        
        let csv = "post_id,post_raw,comment_id,author,created_time,comment_raw\n";
        
        let commentBlocks = getElementsByXPath(commentBlockXPath);
        let validCommentCount = 0;
        let filteredCounts = {
            url: 0,
            emoji: 0,
            emoticon: 0,
            tooShort: 0,
            processed: 0
        };
        
        commentBlocks.forEach((block, idx) => {
            // Main comment
            let mainComment = block.querySelector('#comment');
            if (mainComment) {
                let author = getAuthorName(mainComment);
                let commentText = getCommentText(mainComment);
                
                if (commentText && commentText.trim() !== '') {
                    if (containsURL(commentText)) {
                        filteredCounts.url++;
                        return;
                    }
                    
                    if (!isValidComment(commentText)) {
                        return; // isValidComment đã log chi tiết lý do filter
                    }
                    
                    // Xử lý comment text
                    let processedText = processCommentText(commentText);
                    if (processedText && processedText !== commentText) {
                        filteredCounts.processed++;
                        console.log(`Processed comment: "${commentText}" -> "${processedText}"`);
                    }
                    
                    let timestampRaw = getTimestamp(mainComment);
                    let created_time = convertYouTubeTimeToDate(timestampRaw);
                    
                    validCommentCount++;
                    let comment_id = video_id + '_' + validCommentCount;
                    
                    csv += `${quoteString(video_id)},${quoteString(video_title)},${quoteString(comment_id)},${quoteString(author)},${quoteString(created_time)},${quoteString(processedText || commentText)}\n`;
                }
            }
            
            // Reply comments
            let repliesContainer = block.querySelector('ytd-comment-replies-renderer');
            if (repliesContainer) {
                let replyComments = repliesContainer.querySelectorAll('ytd-comment-view-model');
                replyComments.forEach((replyComment) => {
                    let author = getAuthorName(replyComment);
                    let commentText = getCommentText(replyComment);
                    
                    if (commentText && commentText.trim() !== '') {
                        if (containsURL(commentText)) {
                            filteredCounts.url++;
                            return;
                        }
                        
                        if (!isValidComment(commentText)) {
                            return; // isValidComment đã log chi tiết lý do filter
                        }
                        
                        // Xử lý comment text
                        let processedText = processCommentText(commentText);
                        if (processedText && processedText !== commentText) {
                            filteredCounts.processed++;
                            console.log(`Processed reply: "${commentText}" -> "${processedText}"`);
                        }
                        
                        let timestampRaw = getTimestamp(replyComment);
                        let created_time = convertYouTubeTimeToDate(timestampRaw);
                        
                        validCommentCount++;
                        let comment_id = video_id + '_' + validCommentCount;
                        
                        csv += `${quoteString(video_id)},${quoteString(video_title)},${quoteString(comment_id)},${quoteString(author)},${quoteString(created_time)},${quoteString(processedText || commentText)}\n`;
                    }
                });
            }
        });
        
        copy(csv);
        
        console.log('=== SCRAPING SUMMARY ===');
        console.log(`Total comment threads: ${commentBlocks.length}`);
        console.log(`Valid comments collected: ${validCommentCount}`);
        console.log(`Filtered counts:`);
        console.log(`  - URL comments: ${filteredCounts.url}`);
        console.log(`  - Text processed: ${filteredCounts.processed}`);
        console.log(`CSV copied to clipboard!`);
        console.log('Scraping completed successfully!');
    }

    async function scrapeComments() {
        console.log('Starting YouTube comment scraping...');
        
        scrollToCommentsSection();
        await new Promise(r => setTimeout(r, 3000));
        
        await loadAllComments();
        await expandAllContent();
        await collectData();
    }

    window.YouTubeScraper = {
        loadAllComments: loadAllComments,
        expandAllContent: expandAllContent,
        collectData: collectData,
        scrapeComments: scrapeComments,
        debugButtons: debugButtons
    };

    scrapeComments();
}