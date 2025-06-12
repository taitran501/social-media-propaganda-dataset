with({
    copy
}) {
    var commentsDivXPath                 = '//div[contains(@class, "DivCommentListContainer")]';
    var allCommentsXPath                 = '//div[contains(@class, "DivCommentContentContainer")]';
    var level2CommentsXPath              = '//div[contains(@class, "DivReplyContainer")]';

    var publisherProfileUrlXPath         = '//span[contains(@class, "SpanUniqueId")]';
    var nicknameAndTimePublishedAgoXPath = '//span[contains(@class, "SpanOtherInfos")]';

    // we will filter these later because we have to handle them differently depending on what layout we have
    var likesCommentsSharesXPath         = "//strong[contains(@class, 'StrongText')]";

    var postUrlXPath                     = '//div[contains(@class, "CopyLinkText")]'
    var descriptionXPath                 = '//h4[contains(@class, "H4Link")]/preceding-sibling::div'

    // we need "View" or else this catches "Hide" too
    var viewMoreDivXPath                 = '//p[contains(@class, "PReplyAction") and contains(., "View")]';

    // more reliable than querySelector
    function getElementsByXPath(xpath, parent)
    {
        let results = [];
        let query = document.evaluate(xpath, parent || document,
            null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null);
        for (let i = 0, length = query.snapshotLength; i < length; ++i) {
            results.push(query.snapshotItem(i));
        }
        return results;
    }

    function getAllComments(){
        return getElementsByXPath(allCommentsXPath);
    }

    function quoteString(s) {
        return '"' + String(s).replaceAll('"', '""') + '"';
    }

    function getNickname(comment) {
        return getElementsByXPath('./div[1]/a', comment)[0].outerText;
    }

    function isReply(comment) {
        return comment.parentElement.className.includes('Reply')
    }

// Thay thế hàm formatDate hiện tại bằng hàm này
function formatDate(strDate) {
    if (typeof strDate !== 'undefined' && strDate !== null) {
        // Nếu đã có định dạng mm-dd hoặc dd-mm-yyyy
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
        
        // Xử lý thời gian tương đối
        const now = new Date();
        // Điều chỉnh múi giờ về GMT+7
        const offsetHours = 7 - (-now.getTimezoneOffset() / 60);
        now.setHours(now.getHours() + offsetHours);
        
        let targetDate = new Date(now);
        
        // Xử lý "X ago" format
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
            
            // Định dạng dd-mm-yyyy
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
    function extractNumericStats() {
        var strongTags = getElementsByXPath(likesCommentsSharesXPath);
        // the StrongText class is used on lots of things that aren't likes or comments; the last two or three are what we need
		// if it's a direct URL, shares are displayed, so we want the last three; if not, we only want the last two
        likesCommentsShares = parseInt(strongTags[(strongTags.length - 3)].outerText) ? strongTags.slice(-3) : strongTags.slice(-2);
        return likesCommentsShares;
    }

    function csvFromComment(comment) {
        nickname = getNickname(comment);
        user = getElementsByXPath('./a', comment)[0]['href'].split('?')[0].split('/')[3].slice(1);
        commentText = getElementsByXPath('./div[1]/p', comment)[0].outerText;
        timeCommentedAgo = formatDate(getElementsByXPath('./div[1]/p[2]/span', comment)[0].outerText);
        commentLikesCount = getElementsByXPath('./div[2]', comment)[0].outerText;
        pic = getElementsByXPath('./a/span/img', comment)[0] ? getElementsByXPath('./a/span/img', comment)[0]['src'] : "N/A";
        return quoteString(nickname) + ',' + quoteString(user) + ',' + 'https://www.tiktok.com/@' + user + ','
             + quoteString(commentText) + ',' + timeCommentedAgo + ',' + commentLikesCount + ',' + quoteString(pic);
    }

    // Loading 1st level comments
    var loadingCommentsBuffer = 30; // increase buffer if loading comments takes long and the loop breaks too soon
    var numOfcommentsBeforeScroll = getAllComments().length;
    while (loadingCommentsBuffer > 0) {

        allComments = getAllComments();
        lastComment = allComments[allComments.length - 1];
        lastComment.scrollIntoView(false);

        numOfcommentsAftScroll = getAllComments().length;

        // If number of comments doesn't change after 15 iterations, break the loop.
        if (numOfcommentsAftScroll !== numOfcommentsBeforeScroll) {
            loadingCommentsBuffer = 15;
        } else {
            // direct URLs can get jammed up because there's a recommended videos list that sometimes scrolls first, so scroll the div just in case
            commentsDiv = getElementsByXPath(commentsDivXPath)[0];
            commentsDiv.scrollIntoView(false);
            loadingCommentsBuffer--;
        };
        numOfcommentsBeforeScroll = numOfcommentsAftScroll;
        console.log('Loading 1st level comment number ' + numOfcommentsAftScroll);

        // Wait 0.3 seconds.
        await new Promise(r => setTimeout(r, 300));
    }
    console.log('Opened all 1st level comments');


    // Loading 2nd level comments
    loadingCommentsBuffer = 5; // increase buffer if loading comments takes long and the loop breaks too soon
    while (loadingCommentsBuffer > 0) {
        readMoreDivs = getElementsByXPath(viewMoreDivXPath);
        for (var i = 0; i < readMoreDivs.length; i++) {
            readMoreDivs[i].click();
        }

        await new Promise(r => setTimeout(r, 500));
        if (readMoreDivs.length === 0) {
            loadingCommentsBuffer--;
        } else {
            loadingCommentsBuffer = 5;
        }
        console.log('Buffer ' + loadingCommentsBuffer);
    }
    console.log('Opened all 2nd level comments');


    // Reading all comments, extracting and converting the data to csv
    var comments = getAllComments();
    var level2CommentsLength = getElementsByXPath(level2CommentsXPath).length;
    var publisherProfileUrl = getElementsByXPath(publisherProfileUrlXPath)[0].outerText;
    var nicknameAndTimePublishedAgo = getElementsByXPath(nicknameAndTimePublishedAgoXPath)[0].outerText.replaceAll('\n', ' ').split(' · ');

    // Get post URL (post_id)
    var url = window.location.href.split('?')[0];
    var post_id = url.split('/').pop(); // Lấy phần cuối của URL là ID
    
    // Get post description (post_raw)
    var post_raw = "";
    try {
        post_raw = getElementsByXPath(descriptionXPath)[0].outerText;
    } catch (e) {
        post_raw = "N/A";
    }

    // Cấu trúc mới cho CSV header
    var csv = "post_id,post_raw,comment_id,author,created_time,comment_raw\n";
    
    // Lặp qua từng comment
    for (var i = 0; i < comments.length; i++) {
        var comment = comments[i];
        var nickname = getNickname(comment); // author
        var commentText = getElementsByXPath('./div[1]/p', comment)[0].outerText; // comment_raw
        var timeCommentedAgo = formatDate(getElementsByXPath('./div[1]/p[2]/span', comment)[0].outerText); // created_time
        
        // Tạo comment_id đơn giản bằng cách ghép post_id và index
        var comment_id = post_id + "_" + (i + 1);
        
        // Thêm một dòng mới cho CSV
        csv += quoteString(post_id) + ',' +
               quoteString(post_raw) + ',' +
               quoteString(comment_id) + ',' +
               quoteString(nickname) + ',' +
               quoteString(timeCommentedAgo) + ',' +
               quoteString(commentText) + '\n';
    }
    
    console.log('CSV copied to clipboard!');
    copy(csv);
}
