
const fs = require('fs');
const crypto = require('crypto');
const axios = require('axios')
const asyncPool = require('tiny-async-pool')

const { get, post, put, getFileStream } = require('./fetch_common')
const { AuthClientTwoLegged } = require('forge-apis') //only for token

const APS_BASE_URL = 'https://developer.api.autodesk.com'
const APS_S3_SIGNED_DOWNLOAD = APS_BASE_URL + '/oss/v2/buckets/{0}/objects/{1}/signeds3download'
const APS_S3_SIGNED_UPLOAD = APS_BASE_URL + '/oss/v2/buckets/{0}/objects/{1}/signeds3upload'
const APS_OBJECT_DETAILS = APS_BASE_URL + '/oss/v2/buckets/{0}/objects/{1}/details'

const ASP_CLIENT_ID = '<your client id of APS>'
const ASP_CLIENT_SECRET = '<your client secret of APS>'
const APS_BUCKET_KEY = 'xiaodong-aps-new'
const APS_OBJECT_KEY_for_UPLOAD = 'Audubon_Mechanical-2022.rvt'
const APS_OBJECT_KEY_for_DOWNLOAD = APS_OBJECT_KEY_for_UPLOAD

const UPLOAD_CHUNK_SIZE = 5 * 1024 * 1024; // 5 Mb 



String.prototype.format = function () {
    var args = arguments;
    return this.replace(/\{(\d+)\}/g, function (m, i) {
        return args[i];
    });
};

function httpHeaders(access_token) {
    return {
        Authorization: 'Bearer ' + access_token
    }
}

async function getToken() {
    try {
        // Initialize the 2-legged oauth2 client
        const oAuth2TwoLegged = new AuthClientTwoLegged(
            ASP_CLIENT_ID, ASP_CLIENT_SECRET,
            ['data:write', 'data:read', 'bucket:read', 'bucket:update', 'bucket:create'], true
        )

        const credentials = await oAuth2TwoLegged.authenticate()
        return credentials.access_token

    } catch (e) {
        console.error(`getting token failed: ${e}`)
        return {}
    }

}
async function getS3UploadData(token, bucketKey, objectKey, chunksCount) {
    try {
        const endpoint = chunksCount > 1 ? APS_S3_SIGNED_UPLOAD.format(bucketKey, objectKey) + `?firstPart=${1}&parts=${chunksCount}` :
            APS_S3_SIGNED_UPLOAD.format(bucketKey, objectKey)
        const headers = httpHeaders(token)

        const response = await get(endpoint, headers);
        return response

    } catch (e) {
        console.error(`get S3 download url failed: ${e}`)
        return {}
    }
}

async function completeS3Upload(token, bucketKey, objectKey, uploadKey) {
    try {
        const endpoint = APS_S3_SIGNED_UPLOAD.format(bucketKey, objectKey)
        var headers = httpHeaders(token)
        headers['Content-Type'] = 'application/json'
        headers["x-ads-meta-Content-Type"] =  "application/octet-stream";

        const body = { uploadKey: uploadKey }

        const response = await post(endpoint, headers, JSON.stringify(body));
        return response

    } catch (e) {
        console.error(`complete S3 upload failed: ${e}`)
        return {}
    }
}

async function getS3DownloadData(token, bucketKey, objectKey) {
    try {

        const endpoint = APS_S3_SIGNED_DOWNLOAD.format(bucketKey, objectKey)
        const headers = httpHeaders(token)
        const response = await get(endpoint, headers);
        return response


    } catch (e) {
        console.error(`get S3 download url failed: ${e}`)
        return {}
    }
}

async function getObjectDetails(token, bucketKey, objectKey) {
    try {

        const endpoint = APS_OBJECT_DETAILS.format(bucketKey, objectKey)
        const headers = httpHeaders(token)
        const response = await get(endpoint, headers);
        return response 

    } catch (e) {
        console.error(`get object details  failed: ${e}`)
        return {}
    }
}

async function downloadFile(filePath,url, sha1) {
    try {
        const endpoint = url
        const headers = {}
        const body = await getFileStream(endpoint)
        const stream = fs.createWriteStream(filePath)
        body.pipe(stream)
        stream.on('finish', () => {
            stream.close()
            console.log('file downloaded')
            const fileBuffer = fs.readFileSync(filePath)
            const hashSum = crypto.createHash('sha1');
            hashSum.update(fileBuffer);
            const hex = hashSum.digest('hex');
            console.log(hex);
            if (sha1 === hex) {
                console.log('sha1 checking after downloading: same file')
            } else {
                console.log('sha1 checking after downloading: different file')
            }
        });

    } catch (e) {
        console.error(`download file failed: ${e}`)
        return {}
    }
}

async function uploadloadFile(url, chunk) {

    try {
        const response = await axios.put(url,chunk); 
        return response  
      } catch (e) {
        console.error(`uploadBinary chunk failed: ${e}`)
        return null 
      }

    // try {
    //     const endpoint = url
    //     const headers = {}
    //     const body = await put(endpoint, chunk)

    // } catch (e) {
    //     console.error(`download file failed: ${e}`)
    //     return {}
    // }
}

function readChunkSync(filePath, { length, startPosition }) {
    let buffer = Buffer.alloc(length);
    const fileDescriptor = fs.openSync(filePath, 'r');

    try {
        const bytesRead = fs.readSync(fileDescriptor, buffer, {
            length,
            position: startPosition,
        });

        if (bytesRead < length) {
            buffer = buffer.subarray(0, bytesRead);
        }

        return buffer;
    } finally {
        fs.closeSync(fileDescriptor);
    }
}

(async () => {
 
    const token = await getToken()
    console.log('token===>>:\n' + token) 

    //Upload local file

    //1. calculate chunks
    const filePath = './upload/' +APS_OBJECT_KEY_for_UPLOAD
    const stats = fs.statSync(filePath)
    const fileSizeInBytes = stats.size; 
    const chunksCount = fileSizeInBytes / UPLOAD_CHUNK_SIZE < 1 ? 1 : parseInt(fileSizeInBytes / UPLOAD_CHUNK_SIZE) + 1
    
    //get upload data
    const uploadRes = await getS3UploadData(token, APS_BUCKET_KEY, APS_OBJECT_KEY_for_UPLOAD, chunksCount)
    const uploadKey = uploadRes.uploadKey

    //build async jobs
    var upload_jobs = []
    var chunk_index = 0
    var startPosition = 0

    for (var chunk_index = 0; chunk_index < uploadRes.urls.length; chunk_index++) {
        startPosition = chunk_index * UPLOAD_CHUNK_SIZE 
        const chunk = readChunkSync(filePath, { startPosition: startPosition, length: UPLOAD_CHUNK_SIZE })
        upload_jobs.push({ index: chunk_index, url: uploadRes.urls[chunk_index], chunk: chunk })
    }

    const promiseCreator = async (payload) => {
        try {
            const oneUploadRes = await uploadloadFile(payload.url, payload.chunk)
            if (oneUploadRes) {
                console.log(`upload chunk ${payload.index} succeeded`)
                return { succeess: payload.index }
            }
            else {
                console.log(`upload chunk ${payload.index} succeeded`)
                return { failed: payload.index }

            }
        } catch (e) {
            console.log(`upload chunk ${payload.index}  exception`)
            return { failed: payload.index }
        }
    };

    var succeess_chunks = []
    var failed_chunks = []

    for await (const ms of asyncPool(2, upload_jobs, promiseCreator)) {
        if (ms.failed)
            failed_chunks.push(ms.failed)
        if (ms.succeess)
            succeess_chunks.push(ms.succeess)
    }

    //every chunk has been uploaded
    if (failed_chunks.length == 0) {
        //complete upload
        const completeUploadRes = await completeS3Upload(token, APS_BUCKET_KEY, APS_OBJECT_KEY_for_UPLOAD, uploadKey)
        if(completeUploadRes){

            //compare sha1 
            const fileBuffer = fs.readFileSync(filePath)
            const hashSum = crypto.createHash('sha1');
            hashSum.update(fileBuffer);
            const hex = hashSum.digest('hex');
            console.log(hex);

            const objectDetailsRes = await getObjectDetails(token,APS_BUCKET_KEY,APS_OBJECT_KEY_for_UPLOAD)
            if (objectDetailsRes.sha1 === hex) {
                console.log('sha1 checking after uploading: same file')
            } else {
                console.log('sha1 checking after uploading: different file')
            }

        }else{
            console.log('completed failed')
        } 
    } else {
        console.log('some chunks are not uploaded successfully ') 
        console.log(failed_chunks)
    }  

    //Download file back by S3 signed url 
    const downloadRes = await getS3DownloadData(token,APS_BUCKET_KEY,APS_OBJECT_KEY_for_DOWNLOAD)
    await downloadFile('./download/'+APS_OBJECT_KEY_for_DOWNLOAD,downloadRes.url,downloadRes.sha1)

})()


