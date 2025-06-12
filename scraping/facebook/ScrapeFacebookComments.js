with({
    copy
}) {
    // 1. Xác định node popup chứa post (nếu không có thì fallback về document)
    const popup = document.querySelector('div[role="dialog"]') || document;

    // 2. Selector tổng quát, không phụ thuộc class
    const commentBlockXPath = './/div[@role="article"] | .//div[starts-with(@aria-label, "Comment by")]';
    const authorXPath = './/a[@role="link"] | .//span[@dir="auto"]';
    const commentTextXPath = './/div[@dir="auto"] | .//span[@dir="auto"]';
    const timestampXPath = '(.//a[@role="link"])[last()]';
    const viewMoreRepliesXPath = './/span[contains(text(), "View all") and contains(text(), "repl")] | .//span[contains(text(), "View 1 reply")] | .//span[contains(text(), "replied")] | .//span[contains(text(), "View more replies")]';
    const viewMoreCommentsXPath = './/span[contains(text(), " more comments")]';

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

    // Hàm scroll xuống cuối vùng comment hoặc cuối trang, ưu tiên thanh scroll subwindow lớn nhất
    function scrollToBottom() {
        // Ưu tiên scroll đúng vùng popup chứa comment
        let dialog = document.querySelector('div[role="dialog"]');
        let scrollTarget = null;
        if (dialog) {
            // Tìm div con có overflow-y: auto và scrollHeight lớn nhất
            let candidates = Array.from(dialog.querySelectorAll('div')).filter(div => {
                let style = window.getComputedStyle(div);
                return style.overflowY === 'auto' && div.scrollHeight > 500;
            });
            if (candidates.length > 0) {
                // Chọn div có scrollHeight lớn nhất
                scrollTarget = candidates.reduce((a, b) => a.scrollHeight > b.scrollHeight ? a : b);
            }
        }
        if (!scrollTarget) {
            scrollTarget = dialog || document;
        }
        if (scrollTarget === document) {
            window.scrollTo(0, document.body.scrollHeight);
        } else {
            scrollTarget.scrollTop = scrollTarget.scrollHeight;
        }
    }

    async function autoExpand() {
        let tries = 0;
        while (tries < 20) {
            let moreBtns = [
                ...getElementsByXPath(viewMoreRepliesXPath, popup),
                ...getElementsByXPath(viewMoreCommentsXPath, popup)
            ];
            if (moreBtns.length === 0) break;
            moreBtns.forEach(btn => {
                // Thêm logic để click vào nút "replied"
                if (btn.innerText.toLowerCase().includes('replied')) {
                    // Click vào parent element của nút "replied" để mở replies
                    let parentElement = btn.closest('div[role="button"]') || btn;
                    parentElement.click();
                } else {
                    btn.click();
                }
            });
            scrollToBottom(); // Thêm thao tác scroll sau mỗi lần click
            // Click tất cả các nút 'See more' trong popup (dù là button hay nằm trong text)
            let seeMoreBtns = Array.from(popup.querySelectorAll('*')).filter(el => el.innerText && el.innerText.trim().toLowerCase().includes('see more'));
            seeMoreBtns.forEach(btn => btn.click());
            await new Promise(r => setTimeout(r, 1200));
            tries++;
        }
        // Scroll thêm lần cuối để chắc chắn load hết
        scrollToBottom();
        // Click tất cả các nút 'See more' lần cuối (dù là button hay nằm trong text)
        let seeMoreBtns = Array.from(popup.querySelectorAll('*')).filter(el => el.innerText && el.innerText.trim().toLowerCase().includes('see more'));
        seeMoreBtns.forEach(btn => btn.click());
    }

    function getPostId() {
        let url = window.location.href;
        let match = url.match(/permalink\/(\d+)/) || url.match(/posts\/(\d+)/);
        return match ? match[1] : 'unknown';
    }

    function getPostText() {
        // Lấy nội dung post trong popup
        let el = popup.querySelector('div[data-ad-preview="message"]') || popup.querySelector('div[dir="auto"]');
        return el ? el.innerText : 'N/A';
    }

    // Hàm mới để lấy text từ node nhưng bỏ qua <a> và <img> tags
    function getCommentTextContent(node) {
        if (!node) return '';
        
        // Tạo một bản sao của node để xử lý
        const nodeCopy = node.cloneNode(true);
        
        // Xóa tất cả các <a> tag từ bản sao
        const aTags = nodeCopy.querySelectorAll('a');
        for (let i = 0; i < aTags.length; i++) {
            aTags[i].remove();
        }
        
        // Xóa tất cả các <img> tag từ bản sao
        const imgTags = nodeCopy.querySelectorAll('img');
        for (let i = 0; i < imgTags.length; i++) {
            imgTags[i].remove();
        }
        
        // Lấy text còn lại
        return nodeCopy.innerText.trim();
    }

    // Hàm tìm và lấy author chính xác hơn
    function getAuthorName(block) {
        // Tìm thẻ a có role="link" chứa tên tác giả
        // Trong nhiều trường hợp, có nhiều thẻ a role="link", 
        // nhưng tên tác giả thường nằm trong cấu trúc cụ thể

        // Tìm tất cả các thẻ a role="link"
        const authorLinks = block.querySelectorAll('a[role="link"]');
        
        for (let link of authorLinks) {
            // Tìm span có dir="auto" bên trong thẻ a - đây thường là nơi chứa tên tác giả
            const nameSpan = link.querySelector('span[dir="auto"]');
            if (nameSpan && nameSpan.innerText.trim().length > 0) {
                return nameSpan.innerText.trim();
            }
            
            // Trường hợp cấu trúc lồng nhau sâu hơn
            const nestedSpans = link.querySelectorAll('span span');
            for (let span of nestedSpans) {
                if (span.innerText && span.innerText.trim().length > 0) {
                    return span.innerText.trim();
                }
            }
            
            // Nếu không tìm thấy span con, lấy text của chính thẻ a
            if (link.innerText && link.innerText.trim().length > 0) {
                return link.innerText.trim();
            }
        }
        
        // Fallback: Tìm các span có dir="auto" không nằm trong div[dir="auto"]
        const standaloneSpans = Array.from(block.querySelectorAll('span[dir="auto"]')).filter(span => {
            return !span.closest('div[dir="auto"]');
        });
        
        if (standaloneSpans.length > 0) {
            return standaloneSpans[0].innerText.trim();
        }
        
        return 'Unknown';
    }

    // Hàm tìm và lấy comment text chính xác hơn
    function getCommentText(block) {
        // Bỏ qua thẻ a có role="link" để tránh lấy tên author
        const commentDivs = Array.from(block.querySelectorAll('div[dir="auto"]')).filter(div => {
            // Nếu div này là con của thẻ a, bỏ qua
            return !div.closest('a[role="link"]');
        });
        
        if (commentDivs.length > 0) {
            // Lấy nội dung comment từ div đã lọc, bỏ qua img tags
            return getCommentTextContent(commentDivs[0]);
        }
        
        // Fallback: tìm qua span nếu không tìm thấy div
        const commentSpans = Array.from(block.querySelectorAll('span[dir="auto"]')).filter(span => {
            // Bỏ qua span nằm trong thẻ a
            return !span.closest('a[role="link"]');
        });
        
        if (commentSpans.length > 0) {
            return getCommentTextContent(commentSpans[0]);
        }
        
        return '';
    }

    function convertFbTimeToDate(fbTimeRaw) {
        // Tạo date object với timezone GMT+7
        const date = new Date();
        const gmt7Offset = 7 * 60; // GMT+7 in minutes
        const localOffset = date.getTimezoneOffset();
        const totalOffset = gmt7Offset + localOffset;
        date.setMinutes(date.getMinutes() + totalOffset);

        // Xử lý các trường hợp thời gian tương đối
        if (/^\d+d$/.test(fbTimeRaw)) {
            date.setDate(date.getDate() - parseInt(fbTimeRaw));
        } else if (/^\d+w$/.test(fbTimeRaw)) {
            date.setDate(date.getDate() - parseInt(fbTimeRaw) * 7);
        } else if (/^\d+h$/.test(fbTimeRaw)) {
            date.setHours(date.getHours() - parseInt(fbTimeRaw));
        } else if (/^\d+m$/.test(fbTimeRaw)) {
            date.setMinutes(date.getMinutes() - parseInt(fbTimeRaw));
        } else if (/^\d+s$/.test(fbTimeRaw)) {
            date.setSeconds(date.getSeconds() - parseInt(fbTimeRaw));
        } else {
            return fbTimeRaw;
        }

        // Format date thành dd-mm-yyyy
        const dd = String(date.getDate()).padStart(2, '0');
        const mm = String(date.getMonth() + 1).padStart(2, '0');
        const yyyy = date.getFullYear();
        return `${dd}-${mm}-${yyyy}`;
    }

    async function waitForAllCommentsLoaded(getBlocksFn, maxTries = 30, delay = 800) {
        let lastCount = 0;
        let tries = 0;
        const COMMENT_LIMIT = 2500; // Giảm giới hạn xuống 2500 comments

        while (tries < maxTries) {
            await autoExpand();
            let blocks = getBlocksFn();
            
            // Kiểm tra nếu số lượng comments vượt quá giới hạn
            if (blocks.length > COMMENT_LIMIT) {
                console.log(`Đã đạt giới hạn ${COMMENT_LIMIT} comments. Dừng crawl...`);
                return blocks;
            }

            if (blocks.length === lastCount) {
                tries++;
            } else {
                tries = 0;
            }
            lastCount = blocks.length;
            await new Promise(r => setTimeout(r, delay));
        }
        return getBlocksFn();
    }

    async function scrapeComments() {
        // Đợi mở hết tất cả comment và reply, chỉ bắt đầu crawl khi số lượng không tăng nữa
        let getBlocks = () => getElementsByXPath(commentBlockXPath, popup);
        let commentBlocks = await waitForAllCommentsLoaded(getBlocks, 15, 1000);
        let post_id = getPostId();
        let post_raw = getPostText();
        let csv = "post_id,post_raw,comment_id,author,created_time,comment_raw\n";
        
        // Giới hạn số lượng comments được xử lý
        const COMMENT_LIMIT = 2500; // Giảm giới hạn xuống 2500 comments
        const blocksToProcess = commentBlocks.slice(0, COMMENT_LIMIT);
        
        let validCommentCount = 0;
        
        blocksToProcess.forEach((block, idx) => {
            // Sử dụng hàm mới để lấy author một cách chính xác
            let author = getAuthorName(block);
            
            // Sử dụng hàm mới để lấy comment text một cách chính xác
            let commentText = getCommentText(block);
            
            // Bỏ qua nếu comment trống
            if (!commentText || commentText.trim() === '') {
                return;
            }
            
            let timestampNode = getElementsByXPath(timestampXPath, block)[0];
            let timestampRaw = timestampNode ? timestampNode.innerText : 'N/A';
            let created_time = convertFbTimeToDate(timestampRaw);
            
            validCommentCount++;
            let comment_id = post_id + '_' + validCommentCount;
            
            csv += `${quoteString(post_id)},${quoteString(post_raw)},${quoteString(comment_id)},${quoteString(author)},${quoteString(created_time)},${quoteString(commentText)}\n`;
        });
        
        copy(csv);
        console.log(`CSV copied to clipboard! Đã crawl ${validCommentCount} comment hợp lệ (từ tổng số ${blocksToProcess.length} comment).${commentBlocks.length > COMMENT_LIMIT ? ` (Đã giới hạn từ tổng số ${commentBlocks.length} comments)` : ''}`);
    }

    scrapeComments();
} 