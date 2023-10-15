/* eslint-disable max-lines */
const path = require('path');
const fs = require('fs');
const pageUtils = require('./helpers/utils');
const puppeteer = require('puppeteer');
const chalk = require('chalk').default;
const { createTimer } = require('./helpers/timer');
const wait = require('./helpers/wait');
const tldts = require('tldts');
const readline = require("readline");


const DEFAULT_USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36';
const MOBILE_USER_AGENT = 'Mozilla/5.0 (Linux; Android 10; Pixel 2 XL) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Mobile Safari/537.36';
const SAVE_HOMEPAGE_HTML = true

const DEFAULT_VIEWPORT = {
  width: 1440,//px
  height: 812//px
};
const MOBILE_VIEWPORT = {
  width: 412,
  height: 691,
  deviceScaleFactor: 2,
  isMobile: true,
  hasTouch: true
};

const ENABLE_CMP_EXTENSION = true;
const CMP_ACTION = 'NO_ACTION';
// for debugging: will lunch in window mode instad of headless, open devtools and don't close windows after process finishes
const VISUAL_DEBUG = true;

/**
 * @param {function(...any):void} log
 * @param {string} proxyHost
 * @param {string} executablePath path to chromium executable to use
 */
function openBrowser(log, proxyHost, executablePath) {
  /**
   * @type {import('puppeteer').BrowserLaunchArgumentOptions}
   */
  const args = {
      args: [
          // enable FLoC
          '--enable-blink-features=InterestCohortAPI',
          '--enable-features="FederatedLearningOfCohorts:update_interval/10s/minimum_history_domain_size_required/1,FlocIdSortingLshBasedComputation,InterestCohortFeaturePolicy"',
          '--js-flags="--async-stack-traces --stack-trace-limit 32"'
      ]
  };
  if (VISUAL_DEBUG) {
      args.headless = false;
      args.devtools = true;
  }
  if (proxyHost) {
      let url;
      try {
          url = new URL(proxyHost);
      } catch(e) {
          log('Invalid proxy URL');
      }

      args.args.push(`--proxy-server=${proxyHost}`);
      args.args.push(`--host-resolver-rules="MAP * ~NOTFOUND , EXCLUDE ${url.hostname}"`);
  }
  if (executablePath) {
      // @ts-ignore there is no single object that encapsulates properties of both BrowserLaunchArgumentOptions and LaunchOptions that are allowed here
      args.executablePath = executablePath;
  }

  return puppeteer.launch(args);
}

/**
 * @param {puppeteer.BrowserContext} context
 * @param {URL} url
 * @param {{collectors: import('./collectors/BaseCollector')[], log: function(...any):void, urlFilter: function(string, string):boolean, emulateMobile: boolean, emulateUserAgent: boolean, runInEveryFrame: function():void, maxLoadTimeMs: number, extraExecutionTimeMs: number, blockingMethod: string, specificRequests: string, collectorFlags: Object.<string, string>}} data
 *
 * @returns {Promise<CollectResult>}
 */
async function getSiteData(context, url, {
  collectors,
  log,
  urlFilter,
  emulateUserAgent,
  emulateMobile,
  runInEveryFrame,
  maxLoadTimeMs,
  extraExecutionTimeMs,
  blockingMethod,
  specificRequests,
  collectorFlags,
}) {
  const testStarted = Date.now();

  /**
   * @type {{cdpClient: import('puppeteer').CDPSession, type: string, url: string}[]}
   */
  const targets = [];

  const collectorOptions = {
    context,
    url,
    log,
    collectorFlags
  };

  for (let collector of collectors) {
    const timer = createTimer();

    try {
      // eslint-disable-next-line no-await-in-loop
      await collector.init(collectorOptions);
      log(`${collector.id()} init took ${timer.getElapsedTime()}s`);
    } catch (e) {
      log(chalk.yellow(`${collector.id()} init failed`), chalk.gray(e.message), chalk.gray(e.stack));
    }
  }

  let pageTargetCreated = false;

  // initiate collectors for all contexts (main page, web worker, service worker etc.)
  context.on('targetcreated', async target => {
    // we have already initiated collectors for the main page, so we ignore the first page target
    if (target.type() === 'page' && !pageTargetCreated) {
      pageTargetCreated = true;
      return;
    }

    const timer = createTimer();
    let cdpClient = null;

    try {
      cdpClient = await target.createCDPSession();
    } catch (e) {
      log(chalk.yellow(`Failed to connect to "${target.url()}"`), chalk.gray(e.message), chalk.gray(e.stack));
      return;
    }

    const simpleTarget = { url: target.url(), type: target.type(), cdpClient };
    targets.push(simpleTarget);

    try {
      // we have to pause new targets and attach to them as soon as they are created not to miss any data
      await cdpClient.send('Target.setAutoAttach', { autoAttach: true, waitForDebuggerOnStart: true });
    } catch (e) {
      log(chalk.yellow(`Failed to set "${target.url()}" up.`), chalk.gray(e.message), chalk.gray(e.stack));
      return;
    }

    for (let collector of collectors) {
      try {
        // eslint-disable-next-line no-await-in-loop
        await collector.addTarget(simpleTarget);
      } catch (e) {
        log(chalk.yellow(`${collector.id()} failed to attach to "${target.url()}"`), chalk.gray(e.message), chalk.gray(e.stack));
      }
    }

    try {
      // resume target when all collectors are ready
      await cdpClient.send('Runtime.enable');
      await cdpClient.send('Runtime.runIfWaitingForDebugger');
    } catch (e) {
      log(chalk.yellow(`Failed to resume target "${target.url()}"`), chalk.gray(e.message), chalk.gray(e.stack));
      return;
    }

    log(`${target.url()} (${target.type()}) context initiated in ${timer.getElapsedTime()}s`);
  });

  // Create a new page in a pristine context.
  const page = await context.newPage();

  // const sleep = milliseconds => new Promise(resolve => setTimeout(resolve, milliseconds));

  let numRequests = 0;
  let numRequestsModified = 0;
  let numDecorations = 0;
  let numDecorationsModified = 0;

  let pageInitTime = performance.now();
  const urlMaps = new Array();
  const requestTimes = new Array();



  await page.setRequestInterception(true);
  page.on('request', request => {
    numRequests += 1;
    // const initialTime = performance.now();

    // console.log(request.url())
    // Remove ALL query parameters
    // request.continue()

    if (blockingMethod === 'all') {
      // log('Blocking all')
      request.continue({ url: request.url().split('?')[0] });
    }
    // Remove all third party query parameters

    else if (blockingMethod === 'third') {
      // log('Blocking third')
      if (request.url().indexOf(url.host) === -1) {
        let new_url = request.url().split('?')[0]
        // log(final_time)
        request.continue({ url: request.url().split('?')[0] });
      } else {
        request.continue();
      }
    }

    //  Replace all third party query parameters
    else if (blockingMethod === 'replace') {
      // log('Replacing third')
      if (request.url().indexOf(url.host) === -1) {
        let requestUrl = request.url();
        let queryParamsIndex = requestUrl.indexOf('?');
        if (queryParamsIndex === -1) {
          request.continue();
        } else {
          let readingKeyName = true;
          let newUrl = requestUrl.substring(0, queryParamsIndex);
          let overallOptions = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
          for (let i = queryParamsIndex; i < requestUrl.length; i++) {
            if (requestUrl[i] == '=') {
              readingKeyName = false;
            } else if (!readingKeyName && requestUrl[i] == '&') {
              readingKeyName = true;
            }
            if (overallOptions.indexOf(requestUrl[i]) === -1 || readingKeyName) {
              newUrl += requestUrl[i];
            } else {
              let curChar = requestUrl[i];
              let options = '';
              if ((curChar >= 'A' && curChar <= 'Z') || (curChar >= 'a' && curChar <= 'z')) {
                options += 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
              } else if (curChar >= '0' && curChar <= '9') {
                options += '0123456789';
              }
              // } else if (curChar >= '!' && curChar <= '/') {
              //     options = '!"#$%&\'()*+,-./';
              // }
              let randomChar = options[Math.floor(Math.random() * options.length)];
              newUrl += randomChar;
            }
          }
          // log(newUrl);
          // let finalTime = performance.now() - pageInitTime;
          // log(finalTime);

          request.continue();
          // request.continue({
          //   url: newUrl
          // });
        }
      } else {
        // let finalTime = performance.now() - pageInitTime;
        // log(finalTime);
        console.log("Here");

        request.continue();

      }

    }

    else if (blockingMethod === 'specific' && specificRequests.length != 0) {
      let decorations = specificRequests.split(",");
      let cur_url = request.url();

      // Remove ending / if it exists
      if (cur_url.charAt(cur_url.length - 1) == '/') {
        cur_url = cur_url.substring(0, cur_url.length - 1);
      }

      // Keep track if anywhere in our decoration loop we modify the request
      let foundDecorationInLink = false;

      const countUrl = new URL(request.url());
      const countSearchParams = new URLSearchParams(countUrl.search);

      numDecorations += [...new Set(countSearchParams.keys())].length;
      numDecorations += cur_url.split('/').length - 3;

      // Loop through each decoration that we are blocking to see if they exist in this request
      for (let decoration of decorations) {
        let separated = decoration.split("||");

        if (countUrl.hostname.indexOf(url.host) !== -1 || cur_url.indexOf(separated[0]) == -1)
          continue;

        if (separated[1] === 'fragment') {
          if (cur_url.indexOf('#') != -1) {
            console.log('Blocking specific request fragment for url ' + cur_url)
            cur_url = cur_url.split('#')[0];
            foundDecorationInLink = true;
            numDecorationsModified += 1;
            continue;

          }

            // Else: Is query parameter
        } else if (separated.length === 2) {
          const url = new URL(cur_url);
          const searchParams = new URLSearchParams(url.search);
          if (searchParams.has(separated[1])) {
            searchParams.set(separated[1], 'TUVWXYZ');
            const new_url = new URL(`${url.origin}${url.pathname}?${searchParams}`);
            console.log('Blocking specific request query parameter for url ' + cur_url + ', changing to ' + new_url.href);
            cur_url = new_url.href;
            foundDecorationInLink = true;
            numDecorationsModified += 1;
            continue;
          }

          // Else: Is a path parameter
        } else {
          let splitRequestUrl = cur_url.split('/');

          splitRequestUrl[splitRequestUrl.length - 1] = splitRequestUrl[splitRequestUrl.length - 1].split('?')[0];
          let depth = Number(separated[2]);
          if (splitRequestUrl.length - 3 <= depth) {
            continue;
          }
          if (cur_url.indexOf(separated[0]) == -1 || splitRequestUrl.length < depth + 1) continue;
          let newRequestUrl = '';
          for (let i = 0; i < splitRequestUrl.length; i++) {
            if (i > 0) {
              newRequestUrl += '/';
            }
            if (i - 3 === depth) {
              newRequestUrl += 'TUVWXYZ';
            } else {
              newRequestUrl += splitRequestUrl[i];
            }
          }
          if (cur_url.indexOf('?') != -1) {
            newRequestUrl += '?';
            newRequestUrl += cur_url.split('?')[1];
          }
          console.log('Blocking specific request path parameter for url ' + cur_url + ', changing to ' + newRequestUrl)
          cur_url = newRequestUrl;
          foundDecorationInLink = true;
          numDecorationsModified += 1;
          continue;
        }
      }

      if (foundDecorationInLink) {
        urlMaps.push([request.url(), cur_url]);
        numRequestsModified += 1;
        console.log('Continuing request with ' + cur_url + ', original was ' + request.url());
        request.continue({ url: cur_url });
        return;
      } else {
        request.continue();
        return;
      }
    } else {
      request.continue();
      return;
    }
  });


  // optional function that should be run on every page (and subframe) in the browser context
  if (runInEveryFrame) {
    page.evaluateOnNewDocument(runInEveryFrame);
  }

  // We are creating CDP connection before page target is created, if we create it only after
  // new target is created we will miss some requests, API calls, etc.
  const cdpClient = await page.target().createCDPSession();

  // without this, we will miss the initial request for the web worker or service worker file
  await cdpClient.send('Target.setAutoAttach', { autoAttach: true, waitForDebuggerOnStart: true });

  const initPageTimer = createTimer();
  for (let collector of collectors) {
    try {
      // eslint-disable-next-line no-await-in-loop
      await collector.addTarget({ url: url.toString(), type: 'page', cdpClient });
    } catch (e) {
      log(chalk.yellow(`${collector.id()} failed to attach to page`), chalk.gray(e.message), chalk.gray(e.stack));
    }
  }
  log(`page context initiated in ${initPageTimer.getElapsedTime()}s`);

  if (emulateUserAgent) {
    await page.setUserAgent(emulateMobile ? MOBILE_USER_AGENT : DEFAULT_USER_AGENT);
  }

  await page.setViewport(emulateMobile ? MOBILE_VIEWPORT : DEFAULT_VIEWPORT);

  // if any prompts open on page load, they'll make the page hang unless closed
  page.on('dialog', dialog => dialog.dismiss());

  // catch and report crash errors
  page.on('error', e => log(chalk.red(e.message)));

  let timeout = false;

  try {
    await page.goto(url.toString(), { timeout: maxLoadTimeMs, waitUntil: 'networkidle0' });
  } catch (e) {
    if (e instanceof puppeteer.errors.TimeoutError || (e.name && e.name === 'TimeoutError')) {
      log(chalk.yellow('Navigation timeout exceeded.'));

      for (let target of targets) {
        if (target.type === 'page') {
          // eslint-disable-next-line no-await-in-loop
          await target.cdpClient.send('Page.stopLoading');
        }
      }
      timeout = true;
    } else {
      throw e;
    }
  }

  // if (SAVE_HOMEPAGE_HTML) {
  //   const filePathForHTML = path.join(`./specific_csv_data/${url.hostname}`, `${url.hostname}_after_page_load${blockingMethod}`);
  //   let bodyHTML = await page.evaluate(() => document.body.innerHTML);
  //   fs.writeFileSync(filePathForHTML + '.html', bodyHTML);
  // }

  for (let collector of collectors) {
    const postLoadTimer = createTimer();
    try {
      // eslint-disable-next-line no-await-in-loop
      await collector.postLoad();
      log(`${collector.id()} postLoad took ${postLoadTimer.getElapsedTime()}s`);
    } catch (e) {
      log(chalk.yellow(`${collector.id()} postLoad failed`), chalk.gray(e.message), chalk.gray(e.stack));
    }
  }

  // give website a bit more time for things to settle
  await page.waitForTimeout(extraExecutionTimeMs);

  const finalUrl = page.url();
  /**
   * @type {Object<string, Object>}
   */
  const data = {};

  for (let collector of collectors) {
    const getDataTimer = createTimer();
    try {
      // eslint-disable-next-line no-await-in-loop
      const collectorData = await collector.getData({
        finalUrl,
        urlFilter: urlFilter && urlFilter.bind(null, finalUrl)
      });
      data[collector.id()] = collectorData;
      log(`getting ${collector.id()} data took ${getDataTimer.getElapsedTime()}s`);
    } catch (e) {
      log(chalk.yellow(`getting ${collector.id()} data failed`), chalk.gray(e.message), chalk.gray(e.stack));
      data[collector.id()] = null;
    }
  }

  for (let target of targets) {
    try {
      // eslint-disable-next-line no-await-in-loop
      await target.cdpClient.detach();
    } catch (e) {
      // we don't care that much because in most cases an error here means that target already detached
    }
  }

  if (!VISUAL_DEBUG) {
    await page.close();
  }

  // let pageFinalTime = performance.now() - pageInitTime;

  console.log("Number of requests: " + numRequests)
  console.log("Number of requests modified: " + numRequestsModified)
  console.log("Number of decorations: " + numDecorations)
  console.log("Number of decorations modified: " + numDecorationsModified)
  // console.log("Page total load time in milliseconds: " + pageFinalTime)

  // console.log("---------------------------------------------------")
  for (let pair of urlMaps) {
    console.log("Before: " + pair[0] + "\nAfter: " + pair[1])
    console.log("---------------------------------------------------")
  }
  // for (let time of requestTimes) {
  //   console.log(time)
  // }

  return {
    initialUrl: url.toString(),
    finalUrl,
    timeout,
    testStarted,
    testFinished: Date.now(),
    data
  };
}

/**
 * @param {string} documentUrl
 * @param {string} requestUrl
 * @returns {boolean}
 */
function isThirdPartyRequest(documentUrl, requestUrl) {
  const mainPageDomain = tldts.getDomain(documentUrl);

  return tldts.getDomain(requestUrl) !== mainPageDomain;
}

/**
 * @param {URL} url
 * @param {{collectors?: import('./collectors/BaseCollector')[], log?: function(...any):void, filterOutFirstParty?: boolean, emulateMobile?: boolean, emulateUserAgent?: boolean, proxyHost?: string, browserContext?: puppeteer.BrowserContext, runInEveryFrame?: function():void, executablePath?: string, maxLoadTimeMs?: number, extraExecutionTimeMs?: number, blockingMethod?: string, specificRequests?: string, collectorFlags?: Object.<string, string>}} options
 * @returns {Promise<CollectResult>}
 */
module.exports = async (url, options) => {
  const log = options.log || (() => { });
  const browser = options.browserContext ? null : await openBrowser(log, options.proxyHost, options.executablePath);
  // Create a new incognito browser context.
  const context = options.browserContext || await browser.createIncognitoBrowserContext();

  let data = null;

  const maxLoadTimeMs = options.maxLoadTimeMs || 90000;
  const extraExecutionTimeMs = options.extraExecutionTimeMs || 2500;
  const maxTotalTimeMs = maxLoadTimeMs * 2;

  try {
    data = await wait(getSiteData(context, url, {
      collectors: options.collectors || [],
      log,
      urlFilter: options.filterOutFirstParty === true ? isThirdPartyRequest.bind(null) : null,
      emulateUserAgent: options.emulateUserAgent !== false, // true by default
      emulateMobile: options.emulateMobile,
      runInEveryFrame: options.runInEveryFrame,
      maxLoadTimeMs,
      extraExecutionTimeMs,
      blockingMethod: options.blockingMethod,
      specificRequests: options.specificRequests,
      collectorFlags: options.collectorFlags,
    }), maxTotalTimeMs);
  } catch (e) {
    log(chalk.red('Crawl failed'), e.message, chalk.gray(e.stack));
    throw e;
  } finally {
    // only close the browser if it was created here and not debugging
    if (browser && !VISUAL_DEBUG) {
      await browser.close();
    }
  }

  return data;
};


/**
 * @typedef {Object} CollectResult
 * @property {string} initialUrl URL from which the crawler began the crawl (as provided by the caller)
 * @property {string} finalUrl URL after page has loaded (can be different from initialUrl if e.g. there was a redirect)
 * @property {boolean} timeout true if page didn't fully load before the timeout and loading had to be stopped by the crawler
 * @property {number} testStarted time when the crawl started (unix timestamp)
 * @property {number} testFinished time when the crawl finished (unix timestamp)
 * @property {import('./helpers/collectorsList').CollectorData} data object containing output from all collectors
*/
