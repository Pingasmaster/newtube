// Cypress suite mocks the API responses so we can focus on the SPA behavior
// (layout, rendering, interactions) without needing the Rust backend running
describe('Home page test', () => {
  beforeEach(function () {
    cy.viewport(1280, 720);
    // The app probes for metadata.db early on; respond quickly so boot is fast
    cy.intercept('GET', '/metadata.db', {
      statusCode: 200,
      body: 'stub'
    }).as('metadata');

    cy.fixture('bootstrap.json').then((payload) => {
      this.bootstrapData = payload;
      // `/api/bootstrap` seeds the IndexedDB caches
      cy.intercept('GET', '/api/bootstrap', { body: payload }).as('bootstrap');
      // Secondary endpoints act as fallback when bootstrap is unavailable
      cy.intercept('GET', '/api/videos', { body: payload.videos });
    });

    // Keep the rest of the network noise muted by returning static payloads
    cy.intercept('GET', '/api/shorts', { statusCode: 200, body: [] });
    cy.intercept('GET', '/api/videos/*/comments', { statusCode: 200, body: [] });
    cy.intercept('GET', '/api/shorts/*/comments', { statusCode: 200, body: [] });

    cy.visit('/index.html');
    cy.wait('@bootstrap');
  });

  it('Renders home feed from bootstrap payload', function () {
    // The grid should mirror the fixture so UI regressions stand out quickly
    cy.get('.video-card').should('have.length', this.bootstrapData.videos.length);
    cy.contains('.video-title', 'Demo Video').should('be.visible');
    cy.contains('.video-meta', 'QA Channel').should('be.visible');
  });

  it('Toggles sidebar state via header menu button', () => {
    // Sidebar collapsing changes both the sidebar and content margins
    cy.get('.sidebar').should('have.class', 'state-normal');
    cy.get('.menu-btn').click();
    cy.get('.sidebar').should('have.class', 'state-reduced');
    cy.get('.content').should('have.class', 'margin-reduced');
  });
});
