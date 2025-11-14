// Base Component Class
class Component {
    constructor() {
        this.element = null;
    }

    init() {
        throw new Error('init() must be implemented');
    }

    destroy() {
        if (this.element && this.element.parentNode) {
            this.element.parentNode.removeChild(this.element);
        }
        this.element = null;
    }
}

// Header Component
class Header extends Component {
    constructor(onMenuClick) {
        super();
        this.onMenuClick = onMenuClick;
    }

    init() {
        this.element = document.createElement('header');
        this.element.innerHTML = `
            <div class="header-top">
                <div class="logo">
                    <button class="menu-btn">
                        <svg xmlns="http://www.w3.org/2000/svg" height="24" viewBox="0 0 24 24" width="24" fill="currentColor">
                            <path d="M20 5H4a1 1 0 000 2h16a1 1 0 100-2Zm0 6H4a1 1 0 000 2h16a1 1 0 000-2Zm0 6H4a1 1 0 000 2h16a1 1 0 000-2Z"></path>
                        </svg>
                    </button>
                    <a href="#" class="logo-link">
                        <svg xmlns="http://www.w3.org/2000/svg" width="93" height="20" viewBox="0 0 93 20">
                            <g>
                                <path d="M14.4848 20C14.4848 20 23.5695 20 25.8229 19.4C27.0917 19.06 28.0459 18.08 28.3808 16.87C29 14.65 29 9.98 29 9.98C29 9.98 29 5.34 28.3808 3.14C28.0459 1.9 27.0917 0.94 25.8229 0.61C23.5695 0 14.4848 0 14.4848 0C14.4848 0 5.42037 0 3.17711 0.61C1.9286 0.94 0.954148 1.9 0.59888 3.14C0 5.34 0 9.98 0 9.98C0 9.98 0 14.65 0.59888 16.87C0.954148 18.08 1.9286 19.06 3.17711 19.4C5.42037 20 14.4848 20 14.4848 20Z" fill="#FF0033"></path>
                                <path d="M19 10L11.5 5.75V14.25L19 10Z" fill="white"></path>
                            </g>
                            <g>
                                <path d="M37.1384 18.8999V13.4399L40.6084 2.09994H38.0184L36.6984 7.24994C36.3984 8.42994 36.1284 9.65994 35.9284 10.7999H35.7684C35.6584 9.79994 35.3384 8.48994 35.0184 7.22994L33.7384 2.09994H31.1484L34.5684 13.4399V18.8999H37.1384Z" fill="white"></path>
                                <path d="M44.1003 6.29994C41.0703 6.29994 40.0303 8.04994 40.0303 11.8199V13.6099C40.0303 16.9899 40.6803 19.1099 44.0403 19.1099C47.3503 19.1099 48.0603 17.0899 48.0603 13.6099V11.8199C48.0603 8.44994 47.3803 6.29994 44.1003 6.29994ZM45.3903 14.7199C45.3903 16.3599 45.1003 17.3899 44.0503 17.3899C43.0203 17.3899 42.7303 16.3499 42.7303 14.7199V10.6799C42.7303 9.27994 42.9303 8.02994 44.0503 8.02994C45.2303 8.02994 45.3903 9.34994 45.3903 10.6799V14.7199Z" fill="white"></path>
                                <path d="M52.2713 19.0899C53.7313 19.0899 54.6413 18.4799 55.3913 17.3799H55.5013L55.6113 18.8999H57.6012V6.53994H54.9613V16.4699C54.6812 16.9599 54.0312 17.3199 53.4212 17.3199C52.6512 17.3199 52.4113 16.7099 52.4113 15.6899V6.53994H49.7812V15.8099C49.7812 17.8199 50.3613 19.0899 52.2713 19.0899Z" fill="white"></path>
                                <path d="M62.8261 18.8999V4.14994H65.8661V2.09994H57.1761V4.14994H60.2161V18.8999H62.8261Z" fill="white"></path>
                                <path d="M67.8728 19.0899C69.3328 19.0899 70.2428 18.4799 70.9928 17.3799H71.1028L71.2128 18.8999H73.2028V6.53994H70.5628V16.4699C70.2828 16.9599 69.6328 17.3199 69.0228 17.3199C68.2528 17.3199 68.0128 16.7099 68.0128 15.6899V6.53994H65.3828V15.8099C65.3828 17.8199 65.9628 19.0899 67.8728 19.0899Z" fill="white"></path>
                                <path d="M80.6744 6.26994C79.3944 6.26994 78.4744 6.82994 77.8644 7.73994H77.7344C77.8144 6.53994 77.8744 5.51994 77.8744 4.70994V1.43994H75.3244L75.3144 12.1799L75.3244 18.8999H77.5444L77.7344 17.6999H77.8044C78.3944 18.5099 79.3044 19.0199 80.5144 19.0199C82.5244 19.0199 83.3844 17.2899 83.3844 13.6099V11.6999C83.3844 8.25994 82.9944 6.26994 80.6744 6.26994ZM80.7644 13.6099C80.7644 15.9099 80.4244 17.2799 79.3544 17.2799C78.8544 17.2799 78.1644 17.0399 77.8544 16.5899V9.23994C78.1244 8.53994 78.7244 8.02994 79.3944 8.02994C80.4744 8.02994 80.7644 9.33994 80.7644 11.7299V13.6099Z" fill="white"></path>
                                <path d="M92.6517 11.4999C92.6517 8.51994 92.3517 6.30994 88.9217 6.30994C85.6917 6.30994 84.9717 8.45994 84.9717 11.6199V13.7899C84.9717 16.8699 85.6317 19.1099 88.8417 19.1099C91.3817 19.1099 92.6917 17.8399 92.5417 15.3799L90.2917 15.2599C90.2617 16.7799 89.9117 17.3999 88.9017 17.3999C87.6317 17.3999 87.5717 16.1899 87.5717 14.3899V13.5499H92.6517V11.4999ZM88.8617 7.96994C90.0817 7.96994 90.1717 9.11994 90.1717 11.0699V12.0799H87.5717V11.0699C87.5717 9.13994 87.6517 7.96994 88.8617 7.96994Z" fill="white"></path>
                            </g>
                        </svg>
                    </a>
                </div>
                <div class="search">
                    <div class="search-box">
                        <input type="text" class="search-input" placeholder="Search">
                        <button class="search-btn">
                            <svg viewBox="0 0 24 24" width="24" height="24" fill="currentColor">
                                <path d="M15.5 14h-.79l-.28-.27C15.41 12.59 16 11.11 16 9.5 16 5.91 13.09 3 9.5 3S3 5.91 3 9.5 5.91 16 9.5 16c1.61 0 3.09-.59 4.23-1.57l.27.28v.79l5 4.99L20.49 19zm-6 0C7.01 14 5 11.99 5 9.5S7.01 5 9.5 5 14 7.01 14 9.5 11.99 14 9.5 14"></path>
                            </svg>
                        </button>
                    </div>
                </div>
                <div class="user-actions">
                    <button class="search-icon-btn">
                        <svg viewBox="0 0 24 24" width="24" height="24" fill="currentColor">
                            <path d="M15.5 14h-.79l-.28-.27C15.41 12.59 16 11.11 16 9.5 16 5.91 13.09 3 9.5 3S3 5.91 3 9.5 5.91 16 9.5 16c1.61 0 3.09-.59 4.23-1.57l.27.28v.79l5 4.99L20.49 19zm-6 0C7.01 14 5 11.99 5 9.5S7.01 5 9.5 5 14 7.01 14 9.5 11.99 14 9.5 14"></path>
                        </svg>
                    </button>
                    <div class="user-avatar">U</div>
                </div>
            </div>
        `;

        this.element.querySelector('.menu-btn').addEventListener('click', this.onMenuClick);
        return this.element;
    }
}

// Sidebar Component
class Sidebar extends Component {
    constructor(onStateChange) {
        super();
        this.state = 'normal'; // normal, reduced, none
        this.viewportMode = 'desktop';
        this.isExpanded = true;
        this.onStateChange = typeof onStateChange === 'function' ? onStateChange : null;
        this.resizeHandler = null;
        this.userDataStore = typeof window !== 'undefined' ? window.userDataStore : null;
        this.userDataListener = null;

        // Items shown in reduced state
        this.reducedItems = [
            { icon: '<svg xmlns="http://www.w3.org/2000/svg" height="24" viewBox="0 -960 960 960" width="24" fill="currentColor"><path d="M240-200h120v-240h240v240h120v-360L480-740 240-560v360Zm-80 80v-480l320-240 320 240v480H520v-240h-80v240H160Zm320-350Z"/></svg>', text: 'Home', active: true },
            { icon: '<svg xmlns="http://www.w3.org/2000/svg" height="24" viewBox="0 -960 960 960" width="24" fill="currentColor"><path d="M330-270h80v-340H290v80h40v260Zm200 0h60q33 0 56.5-23.5T670-350v-180q0-33-23.5-56.5T590-610h-60q-33 0-56.5 23.5T450-530v180q0 33 23.5 56.5T530-270Zm0-80v-180h60v180h-60ZM360-840v-80h240v80H360ZM480-80q-74 0-139.5-28.5T226-186q-49-49-77.5-114.5T120-440q0-74 28.5-139.5T226-694q49-49 114.5-77.5T480-800q62 0 119 20t107 58l56-56 56 56-56 56q38 50 58 107t20 119q0 74-28.5 139.5T734-186q-49 49-114.5 77.5T480-80Zm0-80q116 0 198-82t82-198q0-116-82-198t-198-82q-116 0-198 82t-82 198q0 116 82 198t198 82Zm0-280Z"/></svg>', text: 'Shorts' },
            { icon: '<svg xmlns="http://www.w3.org/2000/svg" height="24" viewBox="0 -960 960 960" width="24" fill="currentColor"><path d="M160-80q-33 0-56.5-23.5T80-160v-400q0-33 23.5-56.5T160-640h640q33 0 56.5 23.5T880-560v400q0 33-23.5 56.5T800-80H160Zm0-80h640v-400H160v400Zm240-40 240-160-240-160v320ZM160-680v-80h640v80H160Zm120-120v-80h400v80H280ZM160-160v-400 400Z"/></svg>', text: 'Subscriptions' },
            { icon: '<svg xmlns="http://www.w3.org/2000/svg" height="24" viewBox="0 -960 960 960" width="24" fill="currentColor"><path d="M480-480q81 0 169-16.5T800-540v400q-60 27-146 43.5T480-80q-88 0-174-16.5T160-140v-400q63 27 151 43.5T480-480Zm240 280v-230q-50 14-115.5 22T480-400q-59 0-124.5-8T240-430v230q50 18 115 29t125 11q60 0 125-11t115-29ZM480-880q66 0 113 47t47 113q0 66-47 113t-113 47q-66 0-113-47t-47-113q0-66 47-113t113-47Zm0 240q33 0 56.5-23.5T560-720q0-33-23.5-56.5T480-800q-33 0-56.5 23.5T400-720q0 33 23.5 56.5T480-640Zm0-80Zm0 425Z"/></svg>', text: 'You' },
            { icon: '<svg xmlns="http://www.w3.org/2000/svg" height="24" viewBox="0 -960 960 960" width="24" fill="currentColor"><path d="M480-120q-138 0-240.5-91.5T122-440h82q14 104 92.5 172T480-200q117 0 198.5-81.5T760-480q0-117-81.5-198.5T480-760q-69 0-129 32t-101 88h110v80H120v-240h80v94q51-64 124.5-99T480-840q75 0 140.5 28.5t114 77q48.5 48.5 77 114T840-480q0 75-28.5 140.5t-77 114q-48.5 48.5-114 77T480-120Zm112-192L440-464v-216h80v184l128 128-56 56Z"/></svg>', text: 'History' }
        ];
        
        // Items shown in normal state
        this.normalItems = [
            { icon: '<svg xmlns="http://www.w3.org/2000/svg" height="24" viewBox="0 -960 960 960" width="24" fill="currentColor"><path d="M240-200h120v-240h240v240h120v-360L480-740 240-560v360Zm-80 80v-480l320-240 320 240v480H520v-240h-80v240H160Zm320-350Z"/></svg>', text: 'Home', active: true },
            { icon: '<svg xmlns="http://www.w3.org/2000/svg" height="24" viewBox="0 -960 960 960" width="24" fill="currentColor"><path d="M330-270h80v-340H290v80h40v260Zm200 0h60q33 0 56.5-23.5T670-350v-180q0-33-23.5-56.5T590-610h-60q-33 0-56.5 23.5T450-530v180q0 33 23.5 56.5T530-270Zm0-80v-180h60v180h-60ZM360-840v-80h240v80H360ZM480-80q-74 0-139.5-28.5T226-186q-49-49-77.5-114.5T120-440q0-74 28.5-139.5T226-694q49-49 114.5-77.5T480-800q62 0 119 20t107 58l56-56 56 56-56 56q38 50 58 107t20 119q0 74-28.5 139.5T734-186q-49 49-114.5 77.5T480-80Zm0-80q116 0 198-82t82-198q0-116-82-198t-198-82q-116 0-198 82t-82 198q0 116 82 198t198 82Zm0-280Z"/></svg>', text: 'Shorts' },
            { icon: '<svg xmlns="http://www.w3.org/2000/svg" height="24" viewBox="0 -960 960 960" width="24" fill="currentColor"><path d="M160-80q-33 0-56.5-23.5T80-160v-400q0-33 23.5-56.5T160-640h640q33 0 56.5 23.5T880-560v400q0 33-23.5 56.5T800-80H160Zm0-80h640v-400H160v400Zm240-40 240-160-240-160v320ZM160-680v-80h640v80H160Zm120-120v-80h400v80H280ZM160-160v-400 400Z"/></svg>', text: 'Subscriptions' },
            { divider: true },
            { icon: '<svg xmlns="http://www.w3.org/2000/svg" height="24" viewBox="0 -960 960 960" width="24" fill="currentColor"><path d="M480-480q81 0 169-16.5T800-540v400q-60 27-146 43.5T480-80q-88 0-174-16.5T160-140v-400q63 27 151 43.5T480-480Zm240 280v-230q-50 14-115.5 22T480-400q-59 0-124.5-8T240-430v230q50 18 115 29t125 11q60 0 125-11t115-29ZM480-880q66 0 113 47t47 113q0 66-47 113t-113 47q-66 0-113-47t-47-113q0-66 47-113t113-47Zm0 240q33 0 56.5-23.5T560-720q0-33-23.5-56.5T480-800q-33 0-56.5 23.5T400-720q0 33 23.5 56.5T480-640Zm0-80Zm0 425Z"/></svg>', text: 'You' },
            { icon: '<svg xmlns="http://www.w3.org/2000/svg" height="24" viewBox="0 -960 960 960" width="24" fill="currentColor"><path d="M480-120q-138 0-240.5-91.5T122-440h82q14 104 92.5 172T480-200q117 0 198.5-81.5T760-480q0-117-81.5-198.5T480-760q-69 0-129 32t-101 88h110v80H120v-240h80v94q51-64 124.5-99T480-840q75 0 140.5 28.5t114 77q48.5 48.5 77 114T840-480q0 75-28.5 140.5t-77 114q-48.5 48.5-114 77T480-120Zm112-192L440-464v-216h80v184l128 128-56 56Z"/></svg>', text: 'History' },
            { divider: true },
            { section: 'Explore' },
            { icon: '<svg xmlns="http://www.w3.org/2000/svg" height="24" viewBox="0 -960 960 960" width="24" fill="currentColor"><path d="M400-120q-66 0-113-47t-47-113q0-66 47-113t113-47q23 0 42.5 5.5T480-418v-422h240v160H560v400q0 66-47 113t-113 47Z"/></svg>', text: 'Music' },
            { icon: '<svg xmlns="http://www.w3.org/2000/svg" height="24" viewBox="0 -960 960 960" width="24" fill="currentColor"><path d="m160-800 80 160h120l-80-160h80l80 160h120l-80-160h80l80 160h120l-80-160h120q33 0 56.5 23.5T880-720v480q0 33-23.5 56.5T800-160H160q-33 0-56.5-23.5T80-240v-480q0-33 23.5-56.5T160-800Zm0 240v320h640v-320H160Zm0 0v320-320Z"/></svg>', text: 'Movies & TV' },
            { icon: '<svg xmlns="http://www.w3.org/2000/svg" height="24" viewBox="0 -960 960 960" width="24" fill="currentColor"><path d="M160-400q-33 0-56.5-23.5T80-480q0-33 23.5-56.5T160-560q33 0 56.5 23.5T240-480q0 33-23.5 56.5T160-400Zm66 228-56-56 174-174 56 56-174 174Zm120-388L172-734l56-56 174 174-56 56ZM480-80q-33 0-56.5-23.5T400-160q0-33 23.5-56.5T480-240q33 0 56.5 23.5T560-160q0 33-23.5 56.5T480-80Zm0-640q-33 0-56.5-23.5T400-800q0-33 23.5-56.5T480-880q33 0 56.5 23.5T560-800q0 33-23.5 56.5T480-720Zm134 162-56-58 176-174 56 56-176 176Zm120 386L560-346l56-56 174 174-56 56Zm66-228q-33 0-56.5-23.5T720-480q0-33 23.5-56.5T800-560q33 0 56.5 23.5T880-480q0 33-23.5 56.5T800-400Z"/></svg>', text: 'Live' },
            { icon: '<svg xmlns="http://www.w3.org/2000/svg" height="24" viewBox="0 -960 960 960" width="24" fill="currentColor"><path d="M189-160q-60 0-102.5-43T42-307q0-9 1-18t3-18l84-336q14-54 57-87.5t98-33.5h390q55 0 98 33.5t57 87.5l84 336q2 9 3.5 18.5T919-306q0 61-43.5 103.5T771-160q-42 0-78-22t-54-60l-28-58q-5-10-15-15t-21-5H385q-11 0-21 5t-15 15l-28 58q-18 38-54 60t-78 22Zm3-80q19 0 34.5-10t23.5-27l28-57q15-31 44-48.5t63-17.5h190q34 0 63 18t45 48l28 57q8 17 23.5 27t34.5 10q28 0 48-18.5t21-46.5q0 1-2-19l-84-335q-7-27-28-44t-49-17H285q-28 0-49.5 17T208-659l-84 335q-2 6-2 18 0 28 20.5 47t49.5 19Zm348-280q17 0 28.5-11.5T580-560q0-17-11.5-28.5T540-600q-17 0-28.5 11.5T500-560q0 17 11.5 28.5T540-520Zm80-80q17 0 28.5-11.5T660-640q0-17-11.5-28.5T620-680q-17 0-28.5 11.5T580-640q0 17 11.5 28.5T620-600Zm0 160q17 0 28.5-11.5T660-480q0-17-11.5-28.5T620-520q-17 0-28.5 11.5T580-480q0 17 11.5 28.5T620-440Zm80-80q17 0 28.5-11.5T740-560q0-17-11.5-28.5T700-600q-17 0-28.5 11.5T660-560q0 17 11.5 28.5T700-520Zm-360 60q13 0 21.5-8.5T370-490v-40h40q13 0 21.5-8.5T440-560q0-13-8.5-21.5T410-590h-40v-40q0-13-8.5-21.5T340-660q-13 0-21.5 8.5T310-630v40h-40q-13 0-21.5 8.5T240-560q0 13 8.5 21.5T270-530h40v40q0 13 8.5 21.5T340-460Zm140-20Z"/></svg>', text: 'Gaming' },
            { icon: '<svg xmlns="http://www.w3.org/2000/svg" height="24" viewBox="0 -960 960 960" width="24" fill="currentColor"><path d="M160-120q-33 0-56.5-23.5T80-200v-640l67 67 66-67 67 67 67-67 66 67 67-67 67 67 66-67 67 67 67-67 66 67 67-67v640q0 33-23.5 56.5T800-120H160Zm0-80h280v-240H160v240Zm360 0h280v-80H520v80Zm0-160h280v-80H520v80ZM160-520h640v-120H160v120Z"/></svg>', text: 'News' },
            { icon: '<svg xmlns="http://www.w3.org/2000/svg" height="24" viewBox="0 -960 960 960" width="24" fill="currentColor"><path d="M280-120v-80h160v-124q-49-11-87.5-41.5T296-442q-75-9-125.5-65.5T120-640v-40q0-33 23.5-56.5T200-760h80v-80h400v80h80q33 0 56.5 23.5T840-680v40q0 76-50.5 132.5T664-442q-18 46-56.5 76.5T520-324v124h160v80H280Zm0-408v-152h-80v40q0 38 22 68.5t58 43.5Zm200 128q50 0 85-35t35-85v-240H360v240q0 50 35 85t85 35Zm200-128q36-13 58-43.5t22-68.5v-40h-80v152Zm-200-52Z"/></svg>', text: 'Sports' },
            { icon: '<svg xmlns="http://www.w3.org/2000/svg" height="24" viewBox="0 -960 960 960" width="24" fill="currentColor"><path d="M400-240q-33 0-56.5-23.5T320-320v-50q-57-39-88.5-100T200-600q0-117 81.5-198.5T480-880q117 0 198.5 81.5T760-600q0 69-31.5 129.5T640-370v50q0 33-23.5 56.5T560-240H400Zm0-80h160v-92l34-24q41-28 63.5-71.5T680-600q0-83-58.5-141.5T480-800q-83 0-141.5 58.5T280-600q0 49 22.5 92.5T366-436l34 24v92Zm0 240q-17 0-28.5-11.5T360-120v-40h240v40q0 17-11.5 28.5T560-80H400Zm80-520Z"/></svg>', text: 'Learning' },
            { icon: '<svg xmlns="http://www.w3.org/2000/svg" height="24" viewBox="0 -960 960 960" width="24" fill="currentColor"><path d="M120-160q-17 0-28.5-11.5T80-200q0-10 4-18.5T96-232l344-258v-70q0-17 12-28.5t29-11.5q25 0 42-18t17-43q0-25-17.5-42T480-720q-25 0-42.5 17.5T420-660h-80q0-58 41-99t99-41q58 0 99 40.5t41 98.5q0 47-27.5 84T520-526v36l344 258q8 5 12 13.5t4 18.5q0 17-11.5 28.5T840-160H120Zm120-80h480L480-420 240-240Z"/></svg>', text: 'Fashion & Beauty' },
            { icon: '<svg xmlns="http://www.w3.org/2000/svg" height="24" viewBox="0 -960 960 960" width="24" fill="currentColor"><path d="M440-80v-331q-18-11-29-28.5T400-480q0-33 23.5-56.5T480-560q33 0 56.5 23.5T560-480q0 23-11 41t-29 28v331h-80ZM204-190q-57-55-90.5-129.5T80-480q0-83 31.5-156T197-763q54-54 127-85.5T480-880q83 0 156 31.5T763-763q54 54 85.5 127T880-480q0 86-33.5 161T756-190l-56-56q46-44 73-104.5T800-480q0-134-93-227t-227-93q-134 0-227 93t-93 227q0 69 27 129t74 104l-57 57Zm113-113q-35-33-56-78.5T240-480q0-100 70-170t170-70q100 0 170 70t70 170q0 53-21 99t-56 78l-57-57q25-23 39.5-54t14.5-66q0-66-47-113t-113-47q-66 0-113 47t-47 113q0 36 14.5 66.5T374-360l-57 57Z"/></svg>', text: 'Podcasts' },
            { divider: true },
            { icon: '<svg xmlns="http://www.w3.org/2000/svg" height="24" viewBox="0 -960 960 960" width="24" fill="currentColor"><path d="m370-80-16-128q-13-5-24.5-12T307-235l-119 50L78-375l103-78q-1-7-1-13.5v-27q0-6.5 1-13.5L78-585l110-190 119 50q11-8 23-15t24-12l16-128h220l16 128q13 5 24.5 12t22.5 15l119-50 110 190-103 78q1 7 1 13.5v27q0 6.5-2 13.5l103 78-110 190-118-50q-11 8-23 15t-24 12L590-80H370Zm70-80h79l14-106q31-8 57.5-23.5T639-327l99 41 39-68-86-65q5-14 7-29.5t2-31.5q0-16-2-31.5t-7-29.5l86-65-39-68-99 42q-22-23-48.5-38.5T533-694l-13-106h-79l-14 106q-31 8-57.5 23.5T321-633l-99-41-39 68 86 64q-5 15-7 30t-2 32q0 16 2 31t7 30l-86 65 39 68 99-42q22 23 48.5 38.5T427-266l13 106Zm42-180q58 0 99-41t41-99q0-58-41-99t-99-41q-59 0-99.5 41T342-480q0 58 40.5 99t99.5 41Zm-2-140Z"/></svg>', text: 'Settings' },
            { icon: '<svg xmlns="http://www.w3.org/2000/svg" height="24" viewBox="0 -960 960 960" width="24" fill="currentColor"><path d="M478-240q21 0 35.5-14.5T528-290q0-21-14.5-35.5T478-340q-21 0-35.5 14.5T428-290q0 21 14.5 35.5T478-240Zm-36-154h74q0-33 7.5-52t42.5-52q26-26 41-49.5t15-56.5q0-56-41-86t-97-30q-57 0-92.5 30T342-618l66 26q5-18 22.5-39t53.5-21q32 0 48 17.5t16 38.5q0 20-12 37.5T506-526q-44 39-54 59t-10 73Zm38 314q-83 0-156-31.5T197-197q-54-54-85.5-127T80-480q0-83 31.5-156T197-763q54-54 127-85.5T480-880q83 0 156 31.5T763-763q54 54 85.5 127T880-480q0 83-31.5 156T763-197q-54 54-127 85.5T480-80Zm0-80q134 0 227-93t93-227q0-134-93-227t-227-93q-134 0-227 93t-93 227q0 134 93 227t227 93Zm0-320Z"/></svg>', text: 'Help' }
        ];
        
    }

    setInitialState() {
        this.viewportMode = this.getViewportMode(window.innerWidth);
        this.isExpanded = this.viewportMode === 'desktop';
        this.state = this.deriveState();
    }

    getViewportMode(width) {
        if (width <= 768) {
            return 'mobile';
        }
        if (width <= 1024) {
            return 'tablet';
        }
        return 'desktop';
    }

    deriveState() {
        if (this.viewportMode === 'mobile') {
            return this.isExpanded ? 'normal' : 'none';
        }
        if (this.viewportMode === 'tablet') {
            return this.isExpanded ? 'normal' : 'reduced';
        }
        return this.isExpanded ? 'normal' : 'reduced';
    }

    handleResize() {
        const newMode = this.getViewportMode(window.innerWidth);
        if (newMode === this.viewportMode) {
            return;
        }

        this.viewportMode = newMode;
        this.isExpanded = newMode === 'desktop';
        this.state = this.deriveState();
        this.render();
    }

    toggle() {
        this.isExpanded = !this.isExpanded;
        this.state = this.deriveState();
        this.render();
    }

    render() {
        if (!this.element) {
            return;
        }

        // Clear existing content
        this.element.innerHTML = '';
        
        // Update classes based on state
        this.element.className = 'sidebar';
        
        if (this.state === 'none') {
            this.element.classList.add('state-none');
            this.notifyStateChange();
            return; // Don't render anything
        } else if (this.state === 'reduced') {
            this.element.classList.add('state-reduced');
        } else {
            this.element.classList.add('state-normal');
        }
        
        // Choose which items to display
        const items = this.state === 'reduced' ? this.reducedItems : this.normalItems;
        
        let youInfoInserted = false;
        items.forEach(item => {
            if (item.divider) {
                const divider = document.createElement('div');
                divider.className = 'sidebar-divider';
                this.element.appendChild(divider);
            } else if (item.section) {
                // Section header (only in normal view)
                const section = document.createElement('div');
                section.className = 'sidebar-section';
                section.textContent = item.section;
                this.element.appendChild(section);
            } else {
                const sidebarItem = document.createElement('div');
                sidebarItem.className = 'sidebar-item' + (item.active ? ' active' : '');
                sidebarItem.innerHTML = `
                    <span class="sidebar-icon">${item.icon}</span>
                    <span class="sidebar-text">${item.text}</span>
                `;
                this.element.appendChild(sidebarItem);

                if (
                    !youInfoInserted &&
                    this.state === 'normal' &&
                    item.text === 'You'
                ) {
                    const infoBlock = this.createUserDataInfo();
                    if (infoBlock) {
                        this.element.appendChild(infoBlock);
                        youInfoInserted = true;
                    }
                }
            }
        });

        this.notifyStateChange();
    }

    createUserDataInfo() {
        if (!this.userDataStore) {
            return null;
        }

        const stats = this.userDataStore.getStats();
        const info = document.createElement('div');
        info.className = 'sidebar-info';

        const title = document.createElement('strong');
        title.textContent = 'Your data stays on this device';
        info.appendChild(title);

        const copy = document.createElement('p');
        copy.textContent = 'Watch history, likes, dislikes, playlists, and subscriptions are saved locally in this browser.';
        info.appendChild(copy);

        const sync = document.createElement('p');
        sync.textContent = 'Export a JSON backup to sync across devices, then import it from here on the destination device.';
        info.appendChild(sync);

        const actions = document.createElement('div');
        actions.className = 'sidebar-info-actions';
        const exportBtn = document.createElement('button');
        exportBtn.type = 'button';
        exportBtn.className = 'sidebar-info-btn';
        exportBtn.textContent = 'Export data';
        exportBtn.addEventListener('click', () => this.exportUserData());
        const importBtn = document.createElement('button');
        importBtn.type = 'button';
        importBtn.className = 'sidebar-info-btn';
        importBtn.textContent = 'Import data';
        actions.appendChild(exportBtn);
        actions.appendChild(importBtn);
        info.appendChild(actions);

        const fileInput = document.createElement('input');
        fileInput.type = 'file';
        fileInput.accept = 'application/json';
        fileInput.addEventListener('change', (event) => {
            const files = event.target.files;
            if (files && files[0]) {
                this.importUserData(files[0]);
            }
            event.target.value = '';
        });
        info.appendChild(fileInput);
        importBtn.addEventListener('click', () => fileInput.click());

        const statsBlock = document.createElement('div');
        statsBlock.className = 'sidebar-info-stats';
        [
            ['Likes', stats.likes],
            ['Dislikes', stats.dislikes],
            ['Playlists', stats.playlists],
            ['Subscriptions', stats.subscriptions],
            ['Watched', stats.watched]
        ].forEach(([label, value]) => {
            const stat = document.createElement('span');
            stat.textContent = `${label}: ${value}`;
            statsBlock.appendChild(stat);
        });
        info.appendChild(statsBlock);

        const tip = document.createElement('p');
        tip.textContent = 'Tip: email the export file to yourself or store it in a password manager to keep devices in sync.';
        info.appendChild(tip);

        return info;
    }

    exportUserData() {
        if (this.userDataStore) {
            this.userDataStore.downloadExport();
        }
    }

    importUserData(file) {
        if (!file || !this.userDataStore) {
            return;
        }
        const reader = new FileReader();
        reader.onload = () => {
            try {
                this.userDataStore.importFromString(reader.result);
                alert('✅ User data imported. All likes, playlists, subscriptions, and history now match the file.');
            } catch (error) {
                console.error('❌ Import failed:', error);
                alert('⚠️ Could not import that file. Please provide a ViewTube export JSON.');
            }
        };
        reader.readAsText(file);
    }

    notifyStateChange() {
        if (typeof this.onStateChange === 'function') {
            this.onStateChange(this.state);
        }
    }

    init() {
        this.element = document.createElement('aside');
        this.element.className = 'sidebar';
        this.element.id = 'sidebar';

        this.setInitialState();
        this.resizeHandler = () => this.handleResize();
        window.addEventListener('resize', this.resizeHandler);
        if (this.userDataStore && !this.userDataListener) {
            this.userDataListener = () => this.render();
            window.addEventListener('viewtube:userdata', this.userDataListener);
        }
        
        this.render();
        return this.element;
    }
    
    destroy() {
        if (this.resizeHandler) {
            window.removeEventListener('resize', this.resizeHandler);
        }
        if (this.userDataListener) {
            window.removeEventListener('viewtube:userdata', this.userDataListener);
        }
        super.destroy();
    }
}

// Chips Component
class Chips extends Component {
    constructor() {
        super();
        this.categories = [
            'All', 'Music', 'Gaming', 'News', 'Live', 'JavaScript', 'React', 
            'Cooking', 'Fitness', 'Travel', 'Comedy', 'Movies', 'Technology',
            'Science', 'Education', 'Sports', 'Fashion', 'DIY', 'Animation',
            'Podcasts', 'Nature', 'Documentary', 'Art', 'Business', 'Health',
            'Meditation', 'Photography', 'Cars', 'Politics', 'History', 'Space'
        ];
        this.activeIndex = 0;
        this.scrollPosition = 0;
    }

    init() {
        this.element = document.createElement('div');
        this.element.className = 'chips-container';
        
        // Left scroll button
        const leftBtn = document.createElement('button');
        leftBtn.className = 'chip-scroll-btn left';
        leftBtn.innerHTML = `
            <svg xmlns="http://www.w3.org/2000/svg" height="24" viewBox="0 -960 960 960" width="24" fill="currentColor" style="transform: rotate(180deg);">
                <path d="M647-440H160v-80h487L423-744l57-56 320 320-320 320-57-56 224-224Z"/>
            </svg>
        `;
        leftBtn.addEventListener('click', () => this.scroll('left'));
        
        // Chips wrapper
        const chipsWrapper = document.createElement('div');
        chipsWrapper.className = 'chips';
        
        this.categories.forEach((category, index) => {
            const chip = document.createElement('button');
            chip.className = 'chip' + (index === 0 ? ' active' : '');
            chip.textContent = category;
            chip.addEventListener('click', (e) => this.handleChipClick(e, chipsWrapper));
            chipsWrapper.appendChild(chip);
        });
        
        // Right scroll button
        const rightBtn = document.createElement('button');
        rightBtn.className = 'chip-scroll-btn right';
        rightBtn.innerHTML = `
            <svg xmlns="http://www.w3.org/2000/svg" height="24" viewBox="0 -960 960 960" width="24" fill="currentColor">
                <path d="M647-440H160v-80h487L423-744l57-56 320 320-320 320-57-56 224-224Z"/>
            </svg>
        `;
        rightBtn.addEventListener('click', () => this.scroll('right'));
        
        this.element.appendChild(leftBtn);
        this.element.appendChild(chipsWrapper);
        this.element.appendChild(rightBtn);
        
        this.chipsWrapper = chipsWrapper;
        this.updateScrollButtons();
        
        return this.element;
    }

    scroll(direction) {
        const scrollAmount = 200;
        if (direction === 'left') {
            this.chipsWrapper.scrollBy({ left: -scrollAmount, behavior: 'smooth' });
        } else {
            this.chipsWrapper.scrollBy({ left: scrollAmount, behavior: 'smooth' });
        }
    }

    updateScrollButtons() {
        if (!this.chipsWrapper) return;
        
        const leftBtn = this.element.querySelector('.chip-scroll-btn.left');
        const rightBtn = this.element.querySelector('.chip-scroll-btn.right');
        
        this.chipsWrapper.addEventListener('scroll', () => {
            const { scrollLeft, scrollWidth, clientWidth } = this.chipsWrapper;
            
            // Show/hide left button
            if (scrollLeft > 0) {
                leftBtn.style.display = 'flex';
            } else {
                leftBtn.style.display = 'none';
            }
            
            // Show/hide right button
            if (scrollLeft + clientWidth < scrollWidth - 10) {
                rightBtn.style.display = 'flex';
            } else {
                rightBtn.style.display = 'none';
            }
        });
        
        // Initial check
        leftBtn.style.display = 'none';
        if (this.chipsWrapper.scrollWidth > this.chipsWrapper.clientWidth) {
            rightBtn.style.display = 'flex';
        } else {
            rightBtn.style.display = 'none';
        }
    }

    handleChipClick(e, container) {
        container.querySelector('.chip.active').classList.remove('active');
        e.target.classList.add('active');
    }
}

// Video Grid Component
class VideoGrid extends Component {
    constructor() {
        super();
        this.videos = [];
        this.colors = ['#ff6b6b', '#4ecdc4', '#45b7d1', '#96ceb4', '#ffeaa7', '#dfe6e9', '#6c5ce7', '#fd79a8'];
        this.userDataStore = typeof window !== 'undefined' ? window.userDataStore : null;
        this.userDataListener = null;
    }

    init() {
        this.element = document.createElement('div');
        this.element.className = 'video-grid';
        this.setLoading();

        if (this.userDataStore && !this.userDataListener) {
            this.userDataListener = () => this.renderVideos();
            window.addEventListener('viewtube:userdata', this.userDataListener);
        }
        return this.element;
    }

    setLoading(message = 'Loading videos…') {
        if (!this.element) {
            return;
        }
        this.element.innerHTML = '';
        const placeholder = document.createElement('div');
        placeholder.className = 'video-grid-placeholder';
        placeholder.textContent = message;
        this.element.appendChild(placeholder);
    }

    setVideos(videos = []) {
        this.videos = Array.isArray(videos) ? videos : [];
        if (this.element) {
            this.renderVideos();
        }
    }

    renderVideos() {
        if (!this.element) {
            return;
        }
        this.element.innerHTML = '';

        if (this.videos.length === 0) {
            const placeholder = document.createElement('div');
            placeholder.className = 'video-grid-placeholder';
            placeholder.textContent = 'No videos available yet.';
            this.element.appendChild(placeholder);
            return;
        }

        this.videos.forEach((video, index) => {
            const card = this.createVideoCard(video, index);
            this.element.appendChild(card);
        });
    }

    createVideoCard(video, index) {
        const card = document.createElement('a');
        card.className = 'video-card';
        card.href = `/watch?v=${encodeURIComponent(video.videoid)}`;
        card.dataset.videoId = video.videoid;

        const thumbnail = document.createElement('div');
        thumbnail.className = 'video-thumbnail';
        const thumbUrl =
            video.thumbnailUrl ||
            (Array.isArray(video.thumbnails) && video.thumbnails.length > 0
                ? video.thumbnails[0]
                : null);

        if (thumbUrl) {
            thumbnail.style.backgroundImage = `url(${thumbUrl})`;
            thumbnail.style.backgroundSize = 'cover';
            thumbnail.style.backgroundPosition = 'center';
        } else {
            thumbnail.style.background = this.colors[index % this.colors.length];
        }

        const duration = document.createElement('div');
        duration.className = 'video-duration';
        duration.textContent = this.formatDuration(video.durationText, video.duration);
        thumbnail.appendChild(duration);

        if (this.userDataStore) {
            const progress = this.userDataStore.getWatchProgress(video.videoid);
            if (progress > 0) {
                const progressBar = document.createElement('div');
                progressBar.className = 'video-watch-progress';
                progressBar.style.width = `${Math.min(1, progress) * 100}%`;
                thumbnail.appendChild(progressBar);
                if (progress >= 0.9 || this.userDataStore.isWatched(video.videoid)) {
                    card.classList.add('video-watched');
                }
            }
        }

        const info = document.createElement('div');
        info.className = 'video-info';

        const avatar = document.createElement('div');
        avatar.className = 'channel-avatar';
        avatar.style.background = this.colors[(index + 3) % this.colors.length];
        avatar.textContent = this.getAvatarInitial(video.author);

        const details = document.createElement('div');
        details.className = 'video-details';

        const title = document.createElement('div');
        title.className = 'video-title';
        title.textContent = video.title || 'Untitled video';

        const channel = document.createElement('div');
        channel.className = 'video-meta';
        channel.textContent = video.author || 'Unknown channel';

        const meta = document.createElement('div');
        meta.className = 'video-meta';
        meta.textContent = `${this.formatViews(video.views)} views • ${this.formatTime(
            video.uploadDate
        )}`;

        details.appendChild(title);
        details.appendChild(channel);
        details.appendChild(meta);

        info.appendChild(avatar);
        info.appendChild(details);

        card.appendChild(thumbnail);
        card.appendChild(info);

        return card;
    }

    getAvatarInitial(author) {
        if (!author || typeof author !== 'string') {
            return '•';
        }
        const trimmed = author.trim();
        return trimmed ? trimmed.charAt(0).toUpperCase() : '•';
    }

    formatDuration(durationText, durationSeconds) {
        if (durationText && typeof durationText === 'string') {
            return durationText;
        }
        if (typeof durationSeconds !== 'number') {
            return '';
        }

        const total = Math.max(0, Math.floor(durationSeconds));
        const hours = Math.floor(total / 3600);
        const minutes = Math.floor((total % 3600) / 60);
        const seconds = total % 60;

        if (hours > 0) {
            return `${hours}:${minutes.toString().padStart(2, '0')}:${seconds
                .toString()
                .padStart(2, '0')}`;
        }

        return `${minutes}:${seconds.toString().padStart(2, '0')}`;
    }

    formatViews(views) {
        const value = typeof views === 'number' ? views : Number(views);
        if (!Number.isFinite(value) || value < 0) {
            return '0';
        }
        if (value >= 1_000_000) {
            return `${(value / 1_000_000).toFixed(1).replace(/\.0$/, '')}M`;
        }
        if (value >= 1_000) {
            return `${(value / 1_000).toFixed(1).replace(/\.0$/, '')}K`;
        }
        return value.toString();
    }

    formatTime(uploadDate) {
        if (!uploadDate) {
            return 'Unknown';
        }

        const date = new Date(uploadDate);
        if (Number.isNaN(date.getTime())) {
            return 'Unknown';
        }

        const diffMs = Date.now() - date.getTime();
        if (diffMs < 0) {
            return 'Future';
        }

        const minutes = Math.floor(diffMs / (1000 * 60));
        if (minutes < 1) return 'Just now';
        if (minutes < 60) return `${minutes} minute${minutes === 1 ? '' : 's'} ago`;

        const hours = Math.floor(minutes / 60);
        if (hours < 24) return `${hours} hour${hours === 1 ? '' : 's'} ago`;

        const days = Math.floor(hours / 24);
        if (days < 7) return `${days} day${days === 1 ? '' : 's'} ago`;

        const weeks = Math.floor(days / 7);
        if (weeks < 5) return `${weeks} week${weeks === 1 ? '' : 's'} ago`;

        const months = Math.floor(days / 30);
        if (months < 12) return `${months} month${months === 1 ? '' : 's'} ago`;

        const years = Math.floor(days / 365);
        return `${years} year${years === 1 ? '' : 's'} ago`;
    }

    destroy() {
        if (this.userDataListener) {
            window.removeEventListener('viewtube:userdata', this.userDataListener);
            this.userDataListener = null;
        }
        super.destroy();
    }
}

// Main Content Component
class MainContent extends Component {
    constructor() {
        super();
        this.chips = new Chips();
        this.videoGrid = new VideoGrid();
        this.sidebarState = 'normal';
        this.resizeHandler = null;
    }

    init() {
        this.element = document.createElement('main');
        this.element.className = 'content';
        this.element.id = 'content';

        const chipsElement = this.chips.init();
        const gridElement = this.videoGrid.init();
        
        this.element.appendChild(chipsElement);
        this.element.appendChild(gridElement);

        // Set initial margin based on screen size
        this.updateMargin();
        
        // Listen for window resize
        this.resizeHandler = () => this.updateMargin();
        window.addEventListener('resize', this.resizeHandler);

        return this.element;
    }

    setVideos(videos) {
        if (this.videoGrid) {
            this.videoGrid.setVideos(videos);
        }
    }

    updateMargin() {
        if (!this.element) {
            return;
        }

        const width = window.innerWidth;
        this.element.className = 'content';
        
        if (width <= 768) {
            // Mobile: always full width
            this.element.classList.add('margin-none');
        } else if (width <= 1024) {
            // Tablet: depends on sidebar state
            if (this.sidebarState === 'reduced') {
                this.element.classList.add('margin-reduced');
            } else {
                this.element.classList.add('margin-normal');
            }
        } else {
            // Desktop: depends on sidebar state
            if (this.sidebarState === 'reduced') {
                this.element.classList.add('margin-reduced');
            } else {
                this.element.classList.add('margin-normal');
            }
        }
    }

    setSidebarState(state) {
        this.sidebarState = state;
        this.updateMargin();
    }

    destroy() {
        if (this.resizeHandler) {
            window.removeEventListener('resize', this.resizeHandler);
        }
        this.chips.destroy();
        this.videoGrid.destroy();
        super.destroy();
    }
}

// Home Page Class
class HomePage {
    constructor(services = {}) {
        this.services = Object.assign(
            {
                ready: () => Promise.resolve(),
                getVideos: () => Promise.resolve([]),
                getShorts: () => Promise.resolve([])
            },
            services || {}
        );
        this.container = null;
        this.header = null;
        this.sidebar = null;
        this.content = null;
    }

    async init() {
        this.container = document.getElementById('app');
        const pageContainer = this.render();
        this.container.appendChild(pageContainer);

        try {
            await this.services.ready();
            const videos = await this.services.getVideos();
            this.content.setVideos(videos || []);
        } catch (error) {
            console.error('⚠️ Failed to load home videos:', error);
            this.content.setVideos([]);
        }
    }

    render() {
        // Create page container
        const pageContainer = document.createElement('div');
        pageContainer.className = 'page-home';
        
        // Initialize components
        this.header = new Header(() => this.toggleSidebar());
        this.content = new MainContent();
        this.sidebar = new Sidebar((state) => {
            if (this.content) {
                this.content.setSidebarState(state);
            }
        });

        const headerElement = this.header.init();
        const contentElement = this.content.init();
        const sidebarElement = this.sidebar.init();

        // Add to page container
        pageContainer.appendChild(headerElement);
        pageContainer.appendChild(sidebarElement);
        pageContainer.appendChild(contentElement);

        // Ensure the content margin matches the initial sidebar state
        this.content.setSidebarState(this.sidebar.state);
        
        return pageContainer;
    }

    async refresh() {
        try {
            await this.services.ready();
            const videos = await this.services.getVideos();
            this.content.setVideos(videos || []);
        } catch (error) {
            console.error('⚠️ Failed to refresh home videos:', error);
        }
    }

    toggleSidebar() {
        this.sidebar.toggle();
        this.content.setSidebarState(this.sidebar.state);
    }

    close() {
        // Clean up all components and remove event listeners
        if (this.header) this.header.destroy();
        if (this.sidebar) this.sidebar.destroy();
        if (this.content) this.content.destroy();
        
        // Clear the container
        if (this.container) {
            this.container.innerHTML = '';
        }
        
        console.log('Home page closed');
    }
}

if (typeof window !== 'undefined') {
    window.HomePage = HomePage;
}
