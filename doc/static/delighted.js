/* eslint-disable */
// Delighted
!function(e,t,r,n,a){if(!e[a]){for(var i=e[a]=[],s=0;s<r.length;s++){var c=r[s];i[c]=i[c]||function(e){return function(){var t=Array.prototype.slice.call(arguments);i.push([e,t])}}(c)}i.SNIPPET_VERSION="1.0.1";var o=t.createElement("script");o.type="text/javascript",o.async=!0,o.src="https://d2yyd1h5u9mauk.cloudfront.net/integrations/web/v1/library/"+n+"/"+a+".js";var l=t.getElementsByTagName("script")[0];l.parentNode.insertBefore(o,l)}}(window,document,["survey","reset","config","init","set","get","event","identify","track","page","screen","group","alias"],"Dk30CC86ba0nATlK","delighted");
// Segment
!function(){var analytics=window.analytics=window.analytics||[];if(!analytics.initialize)if(analytics.invoked)window.console&&console.error&&console.error("Segment snippet included twice.");else{analytics.invoked=!0;analytics.methods=["trackSubmit","trackClick","trackLink","trackForm","pageview","identify","reset","group","track","ready","alias","debug","page","once","off","on"];analytics.factory=function(t){return function(){var e=Array.prototype.slice.call(arguments);e.unshift(t);analytics.push(e);return analytics}};for(var t=0;t<analytics.methods.length;t++){var e=analytics.methods[t];analytics[e]=analytics.factory(e)}analytics.load=function(t,e){var n=document.createElement("script");n.type="text/javascript";n.async=!0;n.src="https://cdn.segment.com/analytics.js/v1/"+t+"/analytics.min.js";var a=document.getElementsByTagName("script")[0];a.parentNode.insertBefore(n,a);analytics._loadOptions=e};analytics.SNIPPET_VERSION="4.1.0";
        analytics.load("aGhVvyxnPWlyP71vVl9ZjGWxAtoVGLXX");
    }}();

delighted.survey({
    minTimeOnPage: 180,
    sampleFactor: 0.1,
    properties: {
        project: 'pymongo'
    }
});

// Update Segment
analytics.page({
    path: location.pathname,
    url: location.href,
    project: 'pymongo'
});
