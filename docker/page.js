(function () {
  var backToTop = document.getElementById('backToTop');
  if (backToTop) {
    backToTop.addEventListener('click', function () {
      window.scrollTo({ top: 0, behavior: 'smooth' });
    });
    window.addEventListener('scroll', function () {
      backToTop.classList.toggle('show', window.scrollY > 600);
    }, { passive: true });
  }

  // Measures the real sticky nav height into --nav-h so each section's "On this page" panel
  // sticks flush beneath it instead of a hardcoded guess -- stays correct if the nav ever
  // wraps to two lines on a narrow viewport.
  //
  // getBoundingClientRect() forces a synchronous layout flush if anything on the page has
  // pending, unflushed style changes at the moment it's called (a real Lighthouse "forced
  // reflow" finding on this exact call, since window resize events can fire in a rapid burst
  // -- one per pixel while a user drags a window edge -- each one re-forcing layout before the
  // browser would otherwise have batched it into the next natural paint). Deferred into
  // requestAnimationFrame, with only one measurement ever in flight, so a burst of resize
  // events collapses into a single read-then-write that runs at the browser's own next paint
  // instead of forcing a fresh layout on every single event.
  var siteNavEl = document.querySelector('.siteNav');
  var navHeightRaf = null;
  function updateNavHeight() {
    if (navHeightRaf !== null) return;
    navHeightRaf = requestAnimationFrame(function () {
      navHeightRaf = null;
      if (siteNavEl) {
        document.documentElement.style.setProperty('--nav-h', siteNavEl.getBoundingClientRect().height + 'px');
      }
    });
  }
  updateNavHeight();
  window.addEventListener('resize', updateNavHeight, { passive: true });
  // requestAnimationFrame callbacks are paused for as long as the document stays hidden (a
  // background tab, a prerendered tab not yet swapped in) -- confirmed live: a rAF scheduled
  // while hidden never fires until the tab becomes visible. Re-running the measurement on the
  // visibility transition means a tab that loaded in the background still gets a correct
  // --nav-h the moment someone actually looks at it, instead of being stuck on the CSS
  // fallback (var(--nav-h, 49px)) forever.
  document.addEventListener('visibilitychange', function () {
    if (!document.hidden) updateNavHeight();
  });

  // Active-section highlight in the sticky nav -- only meaningful for same-page hash links
  // (index.html's #pricing/#faq/#contact); PolyWire/PolyAdvisor/Docs are real page links now and
  // simply don't match any on-page section, so they're left alone rather than guessed at.
  var navLinks = document.querySelectorAll('.siteNav a[href^="#"]');
  var sections = Array.prototype.map.call(navLinks, function (a) {
    return document.querySelector(a.getAttribute('href'));
  }).filter(Boolean);
  if ('IntersectionObserver' in window && sections.length) {
    var observer = new IntersectionObserver(function (entries) {
      entries.forEach(function (entry) {
        var link = document.querySelector('.siteNav a[href="#' + entry.target.id + '"]');
        if (!link) return;
        if (entry.isIntersecting) {
          navLinks.forEach(function (a) { a.classList.remove('active'); });
          link.classList.add('active');
        }
      });
    }, { rootMargin: '-64px 0px -70% 0px' });
    sections.forEach(function (s) { observer.observe(s); });
  }

  // Per-section "On this page" scrollspy: as its own subheads scroll by, the matching link
  // gets .active and the sticky (possibly collapsed) summary names the current one -- so
  // "where am I" is answerable without opening the panel.
  if ('IntersectionObserver' in window) {
    document.querySelectorAll('details.onThisPage').forEach(function (panel) {
      var links = panel.querySelectorAll('.otpList a[href^="#"]');
      var current = panel.querySelector('.otpCurrent');
      var targets = Array.prototype.map.call(links, function (a) {
        return document.querySelector(a.getAttribute('href'));
      }).filter(Boolean);
      if (!targets.length) return;
      var subObserver = new IntersectionObserver(function (entries) {
        entries.forEach(function (entry) {
          if (!entry.isIntersecting) return;
          var link = panel.querySelector('.otpList a[href="#' + entry.target.id + '"]');
          if (!link) return;
          links.forEach(function (a) { a.classList.remove('active'); });
          link.classList.add('active');
          if (current) current.textContent = '— ' + link.textContent;
        });
      }, { rootMargin: '-96px 0px -70% 0px' });
      targets.forEach(function (t) { subObserver.observe(t); });
    });
  }

  // Legacy-anchor redirect: PolyWire's deep technical-detail sections used to live on this page
  // at #outcomes, #sharding, etc. -- now they live on polywire.html, and #two-ways-to-assess/
  // #what-you-get/#llm-assist/#admin-console-polyadvisor moved to dms.html (variable/string names
  // below keep their original "polyadvisor" spelling deliberately -- they're the OLD anchor names
  // real external bookmarks/shared links may still use, from before Polyadvisor was renamed to
  // Nexagres DMS; renaming these strings would break exactly the backward-compat this redirect
  // exists for). A bookmarked or shared link to one of the old anchors would otherwise land on
  // this page with nothing to scroll to; this forwards it to the new page's matching anchor
  // instead, once, on load.
  var MOVED_TO_POLYWIRE = ['outcomes', 'use-cases', 'admin-console-polywire', 'cache-vs-round-trip',
    'sharding', 'sqs-enqueue-dequeue', 'rollups', 'observability', 'multi-az', 'security',
    'outages', 'error-handling'];
  var MOVED_TO_POLYADVISOR = ['two-ways-to-assess', 'what-you-get', 'llm-assist', 'admin-console-polyadvisor'];
  var hash = window.location.hash.replace('#', '');
  var onIndexPage = /(^|\/)(index\.html)?$/.test(window.location.pathname);
  if (onIndexPage && hash) {
    if (MOVED_TO_POLYWIRE.indexOf(hash) !== -1) {
      window.location.replace('polywire.html#' + hash);
    } else if (MOVED_TO_POLYADVISOR.indexOf(hash) !== -1) {
      window.location.replace('dms.html#' + hash);
    } else if (hash === 'polyadvisor') {
      // The #polyadvisor section itself was renamed to #dms on this same page (the Polyadvisor
      // -> Nexagres DMS rebrand) -- an in-page hash swap, not a cross-page redirect like the
      // cases above, since the section never left this page.
      window.location.hash = 'dms';
    }
  }

  // Contact form: submits to Formspree via fetch (not a plain form POST) so the visitor
  // never leaves the page or sees Formspree's own confirmation page -- just an inline
  // status message here. No email address appears anywhere in this page's source at all;
  // Formspree endpoint ID below is the only thing that maps to a real inbox,
  // and that mapping lives in Formspree's dashboard, not in anything scrapable here. The
  // hidden `_gotcha` field is Formspree's own honeypot -- a real visitor never sees or fills
  // it (see .hp-field's off-screen CSS), so anything that does is treated as spam and dropped
  // silently on Formspree's side before it ever reaches the inbox. Guarded by contactForm's
  // existence -- only index.html has this form; harmless no-op on polywire.html/dms.html.
  var contactForm = document.getElementById('contactForm');
  var contactStatus = document.getElementById('contactStatus');
  var contactSubmitBtn = document.getElementById('contactSubmitBtn');
  if (contactForm) {
    contactForm.addEventListener('submit', function (e) {
      e.preventDefault();
      contactSubmitBtn.disabled = true;
      contactStatus.className = '';
      contactStatus.textContent = 'Sending…';
      fetch(contactForm.action, {
        method: 'POST',
        body: new FormData(contactForm),
        headers: { 'Accept': 'application/json' }
      }).then(function (response) {
        if (response.ok) {
          contactForm.reset();
          contactStatus.className = 'ok';
          contactStatus.textContent = "Sent -- thanks, we'll get back to you.";
        } else {
          contactStatus.className = 'err';
          contactStatus.textContent = 'Something went wrong sending that. Try again in a moment.';
        }
      }).catch(function () {
        contactStatus.className = 'err';
        contactStatus.textContent = 'Something went wrong sending that. Try again in a moment.';
      }).finally(function () {
        contactSubmitBtn.disabled = false;
      });
    });
  }
})();
